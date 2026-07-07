package sqlpro

import (
	"context"
	"fmt"
	"net/url"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
)

func openLeaseTestDB(t *testing.T) DB {
	t.Helper()
	qv := url.Values{}
	qv.Add("_pragma", "busy_timeout(1000)")
	qv.Add("_pragma", "journal_mode(WAL)")
	d, err := Open("sqlite", filepath.Join(t.TempDir(), "lease.db")+"?"+qv.Encode())
	assert.NoError(t, err)
	t.Cleanup(func() { d.Close() })
	assert.NoError(t, d.Exec(`CREATE TABLE lease_t(x INTEGER)`))
	return d
}

func leaseRowCount(t *testing.T, q Query) (n int) {
	t.Helper()
	assert.NoError(t, q.Query(&n, `SELECT COUNT(*) FROM lease_t`))
	return n
}

// An adopter joins the leased TX on a fresh ctx, sees its uncommitted state,
// and its ExecTX runs as a savepoint whose writes commit with the owner.
func TestLeaseAdoptJoin(t *testing.T) {
	d := openLeaseTestDB(t)

	err := d.ExecTX(context.Background(), func(ctx context.Context) error {
		tx := CtxTX(ctx)
		if err := tx.Exec(`INSERT INTO lease_t(x) VALUES (1)`); err != nil {
			return err
		}
		id, stop := tx.Lease()
		defer stop()

		// fresh ctx, like a re-entrant HTTP request
		actx, release, err := AdoptTX(context.Background(), id)
		if err != nil {
			return err
		}
		defer release()

		// the adopter sees the owner's uncommitted row
		assert.Equal(t, 1, leaseRowCount(t, CtxTX(actx)))

		// nested ExecTX joins via savepoint instead of "unable to nest"
		err = d.ExecTX(actx, func(ctx context.Context) error {
			return CtxTX(ctx).Exec(`INSERT INTO lease_t(x) VALUES (2)`)
		}, nil)
		assert.NoError(t, err)
		assert.Equal(t, 2, leaseRowCount(t, CtxTX(actx)))
		return nil
	}, nil)
	assert.NoError(t, err)

	// both rows committed together by the owner
	assert.Equal(t, 2, leaseRowCount(t, d))
}

// A failing adopter job is rolled back to its savepoint; the owner's TX and
// writes survive untouched.
func TestLeaseAdoptSavepointRollback(t *testing.T) {
	d := openLeaseTestDB(t)

	err := d.ExecTX(context.Background(), func(ctx context.Context) error {
		tx := CtxTX(ctx)
		if err := tx.Exec(`INSERT INTO lease_t(x) VALUES (1)`); err != nil {
			return err
		}
		id, stop := tx.Lease()
		defer stop()

		actx, release, err := AdoptTX(context.Background(), id)
		if err != nil {
			return err
		}
		defer release()

		err = d.ExecTX(actx, func(ctx context.Context) error {
			if err := CtxTX(ctx).Exec(`INSERT INTO lease_t(x) VALUES (2)`); err != nil {
				return err
			}
			return fmt.Errorf("adopter failed")
		}, nil)
		assert.ErrorContains(t, err, "adopter failed")

		// the adopter's write is gone, the owner's remains
		assert.Equal(t, 1, leaseRowCount(t, CtxTX(actx)))
		return nil
	}, nil)
	assert.NoError(t, err)
	assert.Equal(t, 1, leaseRowCount(t, d))
}

// stop() and the owner's Commit/Rollback invalidate the lease.
func TestLeaseInvalidation(t *testing.T) {
	d := openLeaseTestDB(t)

	var stoppedID, endedID string
	err := d.ExecTX(context.Background(), func(ctx context.Context) error {
		tx := CtxTX(ctx)
		var stop func()
		stoppedID, stop = tx.Lease()
		stop()
		_, _, err := AdoptTX(context.Background(), stoppedID)
		assert.ErrorContains(t, err, "unknown or ended lease")

		endedID, _ = tx.Lease() // left open, must die with the commit
		return nil
	}, nil)
	assert.NoError(t, err)

	_, _, err = AdoptTX(context.Background(), endedID)
	assert.ErrorContains(t, err, "unknown or ended lease")
}

// Nesting without adoption keeps the hard error.
func TestLeaseNonAdoptedNestingStillErrors(t *testing.T) {
	d := openLeaseTestDB(t)

	err := d.ExecTX(context.Background(), func(ctx context.Context) error {
		return d.ExecTX(ctx, func(ctx context.Context) error { return nil }, nil)
	}, nil)
	assert.ErrorContains(t, err, "unable to nest transaction")
}
