package sqlpro

import (
	"context"
	"database/sql"
	"fmt"
	"runtime/debug"

	// "github.com/jackc/pgx/v5/stdlib"
	// "github.com/jackc/pgx/v5/stdlib"

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

// ExecTX runs the given function inside a TX. On Postgres in writable TX, the
// lock timeout is set to 60s. For Sqlite, the PRAGMA foreign_keys is set on.
func (db2 *db) ExecTX(ctx context.Context, job func(ctx context.Context) error, opts *sql.TxOptions) (err error) {

	defer func() {
		r := recover()
		if r == nil {
			return
		}
		debug.PrintStack()
		err = fmt.Errorf("sqlpro.ExecTX: panic caught: %v", r)
	}()

	select {
	case <-ctx.Done():
		return errors.New("sqlpro.ExecTX: context is done, not starting a transaction")
	default:
	}

	if CtxTX(ctx).ActiveTX() {
		return errors.New("sqlpro.ExecTX: unable to nest transaction")
	}

	conn, err := db2.sqlDB.Conn(ctx)
	if err != nil {
		return fmt.Errorf("sqlpro.ExecTX: conn: %w", err)
	}

	defer func() {
		err2 := conn.Close()
		if err2 != nil {
			err = fmt.Errorf("%w close: %w", err, err2)
		}
	}()

	// Capture the driverConn. This is discouraged in the documnetation of
	// "Raw", but there is no other way to do what we need here. We need to have
	// the original driver connection while still be able to start a sql package
	// transaction. Since the sqlpro package knows that the connection is not
	// shared anywhere else, it is safe to use the connection outside the Raw
	// func in our case. Running the code inside the Raw func doesn't work due
	// to locking which the sql package does.
	var driverConn any
	err = conn.Raw(func(driverConn2 any) (err error) {
		driverConn = driverConn2
		return nil
	})
	if err != nil {
		return err
	}

	tx, err := db2.txBeginContext(ctx, conn, opts)
	if err != nil {
		return fmt.Errorf("sqlpro.ExecTX: begin: %w", err)
	}
	// capture the driverConn, so that insertBulk can use the faster copyFrom (POSTGRES only)

	tx.driverConn = driverConn
	// In a writable TX, set some defaults
	if tx.IsWriteMode() {
		switch tx.Driver() {
		case POSTGRES:
			err = tx.ExecContext(ctx, `SET LOCAL lock_timeout = '60s'`)
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
	err = job(CtxWithTX(ctx, tx))
	if err != nil {
		return rollback(tx, err)
	} else {
		return commit(tx)
	}
	// })
	// if err != nil {
	// 	return err
	// }
	return nil
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
