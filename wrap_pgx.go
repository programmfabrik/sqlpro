package sqlpro

import (
	"context"
	"database/sql"
	"database/sql/driver"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/stdlib"
)

// var _ dbWrappable = wrapPgxTx{}

type wrapPgxTx struct {
	dbWrappable
	tx      pgx.Tx
	conn    pgx.Conn
	stdConn *stdlib.Conn
}

func (wpt wrapPgxTx) ExecContext(ctx context.Context, query string, args ...any) (sql.Result, error) {
	ct, err := wpt.tx.Exec(ctx, query, args...)
	return driver.RowsAffected(ct.RowsAffected()), err
}

func (wpt wrapPgxTx) Exec(query string, args ...any) (sql.Result, error) {
	return wpt.ExecContext(context.Background(), query, args...)
}

// type dbWrappable interface {
// 	Query(query string, args ...interface{}) (*sql.Rows, error)
// 	QueryContext(ctx context.Context, query string, args ...interface{}) (*sql.Rows, error)
// 	Exec(query string, args ...interface{}) (sql.Result, error)
// 	ExecContext(ctx context.Context, query string, args ...interface{}) (sql.Result, error)
// }
