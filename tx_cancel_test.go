package sqlpro

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

const pgTestDSN = "host=localhost port=5432 dbname=apitest password=egal sslmode=disable"

// openPG opens the local postgres used by the COPY tests, or skips.
func openPG(t *testing.T) DB {
	t.Helper()
	db, err := Open(POSTGRES, pgTestDSN)
	if err != nil {
		t.Skipf("no local postgres: %s", err)
	}
	var one int
	if err := db.Query(&one, `SELECT 1`); err != nil {
		db.Close()
		t.Skipf("no local postgres: %s", err)
	}
	return db
}

// A cancelled caller context must not end the transaction underneath the
// running job. Against the fake driver (fakedb_test.go), so it runs without a
// database: database/sql's own ctx rollback would show up as a ROLLBACK
// recorded while the job is still running.
func TestFakeExecTXCancelRollsBackOnlyAfterJob(t *testing.T) {
	db, backend := newFakeSqlPro(t, POSTGRES)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	jobErr := errors.New("job gave up")
	var duringJob []string

	err := db.ExecTX(ctx, func(ctx context.Context) error {
		cancel()
		time.Sleep(200 * time.Millisecond)
		duringJob = backend.recorded("rollback")
		return jobErr
	}, nil)

	assert.Empty(t, duringJob, "the TX must still be open while the job runs")
	assert.Equal(t, []string{"ROLLBACK"}, backend.recorded("rollback"),
		"exactly one rollback, issued by ExecTX after the job returned")
	assert.ErrorIs(t, err, jobErr)
	assert.NotErrorIs(t, err, sql.ErrTxDone)
}

// Same invariant against a real postgres and the pgx driver, which is where
// the concurrent rollback panics.
func TestExecTXCancelKeepsTXUntilJobReturns(t *testing.T) {
	db := openPG(t)
	defer db.Close()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	jobErr := errors.New("job gave up")
	var afterCancelErr error

	err := db.ExecTX(ctx, func(ctx context.Context) error {
		tx := CtxTX(ctx)

		if err := tx.ExecContext(ctx, `CREATE TEMP TABLE cancel_example (id int)`); err != nil {
			return err
		}
		if err := tx.ExecContext(ctx, `INSERT INTO cancel_example (id) VALUES (1)`); err != nil {
			return err
		}

		// The caller goes away while the job is between statements.
		cancel()
		time.Sleep(200 * time.Millisecond)

		// The TX is still ours: a statement on a live context must still run.
		// With database/sql's own ctx rollback this fails with sql.ErrTxDone.
		var n int
		afterCancelErr = tx.QueryContext(context.WithoutCancel(ctx), &n,
			`SELECT count(*) FROM cancel_example`)
		if afterCancelErr == nil {
			assert.Equal(t, 1, n)
		}

		return jobErr
	}, nil)

	assert.NoError(t, afterCancelErr, "TX must still be usable after the caller cancelled")
	assert.ErrorIs(t, err, jobErr)
	assert.NotErrorIs(t, err, sql.ErrTxDone,
		"rollback must be ExecTX's own, not a leftover of database/sql's ctx rollback")
}

// Cancelling in-flight statements must abort the job and roll back, without a
// concurrent rollback racing the driver connection. Run with -race.
func TestExecTXCancelInFlightRace(t *testing.T) {
	db := openPG(t)
	defer db.Close()

	const rounds = 60

	var wg sync.WaitGroup
	for i := range rounds {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()

			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()

			// stagger the cancel across the statement's runtime
			time.AfterFunc(time.Duration(i%20)*time.Millisecond, cancel)

			err := db.ExecTX(ctx, func(ctx context.Context) error {
				tx := CtxTX(ctx)
				var n int
				return tx.QueryContext(ctx, &n, `SELECT pg_sleep(0.02), 1`)
			}, nil)
			// cancelled or completed, both fine — the point is no panic and
			// no data race
			if err != nil && !errors.Is(err, context.Canceled) {
				t.Errorf("round %d: unexpected error: %s", i, err)
			}
		}(i)
	}
	wg.Wait()
}

// The COPY path bypasses database/sql's per-connection lock, so a bulk insert
// must serialize against statements an adopted goroutine runs on the same TX.
// Run with -race.
func TestCopyFromConcurrentWithAdoptedStatement(t *testing.T) {
	db := openPG(t)
	defer db.Close()

	ctx := context.Background()

	err := db.ExecTX(ctx, func(ctx context.Context) error {
		tx := CtxTX(ctx)
		if err := tx.ExecContext(ctx, `CREATE TEMP TABLE copy_race (id SERIAL PRIMARY KEY, name TEXT, value INTEGER)`); err != nil {
			return err
		}

		id, stop := tx.Lease()
		defer stop()

		var wg sync.WaitGroup
		errs := make([]error, 2)

		wg.Add(1)
		go func() {
			defer wg.Done()
			ctx2, release, err := AdoptTX(ctx, id)
			if err != nil {
				errs[0] = err
				return
			}
			defer release()
			errs[0] = db.ExecTX(ctx2, func(ctx context.Context) error {
				for i := range 20 {
					var n int
					if err := CtxTX(ctx).QueryContext(ctx, &n,
						fmt.Sprintf(`SELECT %d`, i)); err != nil {
						return err
					}
				}
				return nil
			}, nil)
		}()

		wg.Add(1)
		go func() {
			defer wg.Done()
			rows := make([]row, 0, 200)
			for i := range 200 {
				rows = append(rows, row{Name: fmt.Sprintf("n%d", i), Value: i})
			}
			errs[1] = tx.InsertBulkContext(ctx, "copy_race", rows)
		}()

		wg.Wait()
		return errors.Join(errs...)
	}, nil)

	assert.NoError(t, err)
}
