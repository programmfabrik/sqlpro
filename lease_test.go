package sqlpro

import (
	"context"
	"database/sql"
	"fmt"
	"net/url"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"

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
// and its ExecTX runs directly on the owner's TX; its writes commit with the
// owner.
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

		// nested ExecTX joins the TX instead of "unable to nest"
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

// A failing write-intent adopter job fails the whole leased TX: the owner's
// Commit rolls back and returns the adopter's error — no partial state, not
// even the owner's own writes, survives.
func TestLeaseAdoptWriteErrorFailsTX(t *testing.T) {
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

		// a further join on the failed TX is refused
		err = d.ExecTX(actx, func(ctx context.Context) error { return nil }, nil)
		assert.ErrorContains(t, err, "adopted transaction has failed")

		// the owner's job succeeds — the failure surfaces at commit
		return nil
	}, nil)
	assert.ErrorContains(t, err, "transaction failed by adopted join")
	assert.ErrorContains(t, err, "adopter failed")

	// nothing committed
	assert.Equal(t, 0, leaseRowCount(t, d))
}

// A failing read-only join reports its error but leaves the TX healthy: the
// owner commits normally.
func TestLeaseAdoptReadErrorKeepsTX(t *testing.T) {
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
			return fmt.Errorf("read probe failed")
		}, &sql.TxOptions{ReadOnly: true})
		assert.ErrorContains(t, err, "read probe failed")
		return nil
	}, nil)
	assert.NoError(t, err)
	assert.Equal(t, 1, leaseRowCount(t, d))
}

// A panicking write-intent adopter job also fails the leased TX.
func TestLeaseAdoptPanicFailsTX(t *testing.T) {
	d := openLeaseTestDB(t)

	err := d.ExecTX(context.Background(), func(ctx context.Context) error {
		tx := CtxTX(ctx)
		id, stop := tx.Lease()
		defer stop()

		actx, release, err := AdoptTX(context.Background(), id)
		if err != nil {
			return err
		}
		defer release()

		err = d.ExecTX(actx, func(ctx context.Context) error {
			panic("adopter exploded")
		}, nil)
		assert.ErrorContains(t, err, "panic caught")
		return nil
	}, nil)
	assert.ErrorContains(t, err, "transaction failed by adopted join")
	assert.ErrorContains(t, err, "adopter exploded")
}

// stop() blocks until an in-flight adopter has released, so the owner never
// uses or ends the TX concurrently with an adopted request; an adopter
// arriving after stop is refused.
func TestLeaseStopWaitsForAdopter(t *testing.T) {
	d := openLeaseTestDB(t)

	err := d.ExecTX(context.Background(), func(ctx context.Context) error {
		tx := CtxTX(ctx)
		id, stop := tx.Lease()

		adopted := make(chan struct{})
		var done atomic.Bool
		go func() {
			actx, release, err := AdoptTX(context.Background(), id)
			assert.NoError(t, err)
			close(adopted)
			// simulate an in-flight adopted request working on the TX
			assert.NoError(t, d.ExecTX(actx, func(ctx context.Context) error {
				time.Sleep(150 * time.Millisecond)
				return CtxTX(ctx).Exec(`INSERT INTO lease_t(x) VALUES (7)`)
			}, nil))
			done.Store(true)
			release()
		}()

		<-adopted
		stop() // must block until the adopter released
		assert.True(t, done.Load(), "stop returned while the adopter was still in flight")

		// after stop, adoption is refused
		_, _, err := AdoptTX(context.Background(), id)
		assert.ErrorContains(t, err, "unknown or ended lease")
		return nil
	}, nil)
	assert.NoError(t, err)

	// the adopter's write, finished before stop returned, committed with the owner
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
