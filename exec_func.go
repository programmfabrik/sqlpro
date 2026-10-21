package sqlpro

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"fmt"
	"os"
	"runtime/debug"
	"sync"

	"github.com/jackc/pgx/v5/stdlib"
	"github.com/pkg/errors"
)

const ctxTX int = 0

// CtxWithTX returns ctx with TX stored
func CtxWithTX(ctx context.Context, tx TX) context.Context {
	return context.WithValue(ctx, ctxTX, tx)
}

// CtxTX returns the TX stored in ctx
func CtxTX(ctx context.Context) TX {
	v := ctx.Value(ctxTX)
	if v == nil {
		var tx *db
		// do not return a <nil> interface untypes
		return tx
	}
	return v.(TX)
}

// ExecTX runs the given function inside a TX: commit if it returns nil,
// rollback otherwise. On postgres a writable TX runs with a lock_timeout of
// 300s; on sqlite the PRAGMA defer_foreign_keys is switched on (foreign key
// checks deferred until commit).
func (db2 *db) ExecTX(ctx context.Context, job func(ctx context.Context) error, opts *sql.TxOptions) (err error) {

	select {
	case <-ctx.Done():
		return fmt.Errorf("sqlpro.ExecTX: context is done, not starting a transaction: %w", ctx.Err())
	default:
	}

	if tx := CtxTX(ctx); tx.ActiveTX() {
		// Joining an existing TX is only legal when it was explicitly adopted
		// (AdoptTX); the job then runs directly on the owner's TX and commit
		// stays with the owner. opts only convey the join's intent: a failed
		// write-intent join fails the whole leased TX, a ReadOnly join's
		// error does not. Anything else is a bug.
		if ctxAdopted(ctx) {
			return execTXJoin(ctx, tx.(*db), job, opts)
		}
		return errors.New("sqlpro.ExecTX: unable to nest transaction")
	}

	// A cancelled caller gets its cancellation back, whatever the driver made
	// of it: pgx reports a statement the cancel stopped before sending as
	// driver.ErrBadConn, and one it interrupted as an i/o timeout.
	defer func() {
		if err != nil && ctx.Err() != nil && !errors.Is(err, ctx.Err()) {
			err = fmt.Errorf("%w: %w", ctx.Err(), err)
		}
	}()

	conn, err := db2.sqlDB.Conn(ctx)
	if err != nil {
		return fmt.Errorf("sqlpro.ExecTX: conn: %w", err)
	}

	var driverConn any
	// Commit and Rollback hand conn back to the pool before their after-hooks
	// run (see endConn); this covers the ways out that end neither.
	releaseConn := sync.OnceValue(func() error { return putConn(conn, driverConn) })
	defer func() {
		err2 := releaseConn()
		if err2 != nil {
			err = fmt.Errorf("%w close: %w", err, err2)
		}
	}()

	// Capture the driverConn. This is discouraged in the documentation of
	// "Raw", but there is no other way to do what we need here. We need to have
	// the original driver connection while still be able to start a sql package
	// transaction. Since the sqlpro package knows that the connection is not
	// shared anywhere else, it is safe to use the connection outside the Raw
	// func in our case. Running the code inside the Raw func doesn't work due
	// to locking which the sql package does.
	err = conn.Raw(func(driverConn2 any) (err error) {
		driverConn = driverConn2
		return nil
	})
	if err != nil {
		return err
	}

	// Begin on an uncancellable context. database/sql otherwise rolls the
	// transaction back from its own awaitDone goroutine the moment ctx is
	// done, concurrently with this goroutine's use of the same driver
	// connection — pgx guards PgConn.Close and asyncClose with a plain
	// unsynchronized status byte and panics "close of closed channel" when
	// both run. ExecTX owns the whole lifecycle (commit and rollback below),
	// and the job keeps the cancellable ctx, so a cancelled caller still
	// aborts the in-flight statement and unwinds into the rollback here.
	tx, err := db2.txBeginContext(context.WithoutCancel(ctx), conn, opts)
	if err != nil {
		return fmt.Errorf("sqlpro.ExecTX: begin: %w", err)
	}
	// capture the driverConn, so that insertBulk can use the faster copyFrom (POSTGRES only)

	tx.driverConn = driverConn
	tx.releaseConn = releaseConn
	// In a writable TX, set some defaults
	if tx.IsWriteMode() {
		switch tx.Driver() {
		case POSTGRES:
			err = tx.ExecContext(ctx, `SET LOCAL lock_timeout = '300s'`)
			if err != nil {
				return rollback(tx, fmt.Errorf("sqlpro.ExecTX: %w", err))
			}
		case SQLITE3:
			err = tx.ExecContext(ctx, `PRAGMA defer_foreign_keys='ON'`)
			if err != nil {
				return rollback(tx, fmt.Errorf("sqlpro.ExecTX: %w", err))
			}
		}
	}
	err = func() (err error) {
		defer func() {
			r := recover()
			if r == nil {
				return
			}
			err = fmt.Errorf("sqlpro.ExecTX: panic caught: %v", r)
			fmt.Fprint(os.Stderr, err.Error()+"\n")
			debug.PrintStack()
		}()
		return job(CtxWithTX(ctx, tx))
	}()
	if err != nil {
		return rollback(tx, err)
	}
	return commit(tx)
}

// putConn returns the connection ExecTX reserved to the pool, or drops it
// when pgx has closed it: a cancelled context closes the pgx connection of the
// statement it interrupts, and database/sql cannot tell (pgx implements no
// driver.Validator). For a transaction begun on the caller's context
// database/sql drops the connection on cancel itself, but ExecTX begins on an
// uncancellable one.
func putConn(conn *sql.Conn, driverConn any) error {
	if sc, ok := driverConn.(*stdlib.Conn); ok && sc.Conn().IsClosed() {
		// ErrBadConn from Raw makes database/sql close the connection
		_ = conn.Raw(func(any) error { return driver.ErrBadConn })
		return nil
	}
	return conn.Close()
}

func rollback(tx TX, err error) error {
	err2 := tx.Rollback()
	if err2 != nil {
		return fmt.Errorf("%w rollback: %w", err, err2)
	} else {
		return err
	}
}

func commit(tx TX) error {
	err2 := tx.Commit()
	if err2 != nil {
		return fmt.Errorf("commit: %w", err2)
	}
	return nil
}
