package sqlpro

import (
	"context"
	"database/sql"
	"fmt"
	"reflect"
	"strings"

	"github.com/pkg/errors"
)

type DB struct {
	db                    dbWrappable
	sqlDB                 *sql.DB // this can be <nil>
	sqlTx                 *sql.Tx // this can be <nil>
	Debug                 bool
	DebugExec             bool
	DebugQuery            bool
	PlaceholderMode       PlaceholderMode
	PlaceholderEscape     rune
	PlaceholderValue      rune
	PlaceholderKey        rune
	MaxPlaceholder        int
	UseReturningForLastId bool
	SupportsLastInsertId  bool
	Driver                string
	DSN                   string
	isClosed              bool

	txWriteMode bool

	// txStart     time.Time
	// transID   int
	LastError error // This is set to the last error

	txAfterCommit   []func()
	txAfterRollback []func()
}

// DB returns the wrapped sql.DB handle
func (db *DB) DB() *sql.DB {
	return db.sqlDB
}

func (db *DB) TX() *sql.Tx {
	return db.sqlTx
}

func (db *DB) String() string {
	return fmt.Sprintf("[%s, %p]", db.Driver, db)
}

type PlaceholderMode int

const (
	DOLLAR PlaceholderMode = iota + 1
	QUESTION
)

type dbWrappable interface {
	Query(query string, args ...interface{}) (*sql.Rows, error)
	QueryContext(ctx context.Context, query string, args ...interface{}) (*sql.Rows, error)
	Exec(query string, args ...interface{}) (sql.Result, error)
	ExecContext(ctx context.Context, query string, args ...interface{}) (sql.Result, error)
}

// NewSqlPro returns a wrapped database handle providing
// access to the sql pro functions.
func newDB(dbWrap dbWrappable) *DB {
	db := new(DB)
	db.db = dbWrap

	// DEFAULTs for sqlite
	db.PlaceholderMode = QUESTION
	db.PlaceholderValue = '?'
	db.PlaceholderEscape = '\\'
	db.PlaceholderKey = '@'
	db.DebugExec = false
	db.MaxPlaceholder = 100
	db.SupportsLastInsertId = true
	db.UseReturningForLastId = false

	return db
}

func (db *DB) Esc(s string) string {
	return `"` + strings.ReplaceAll(s, `"`, `""`) + `"`
}

func (db *DB) EscValue(s string) string {
	return `'` + strings.ReplaceAll(s, `'`, `''`) + `'`
}

// Log returns a copy with debug enabled
func (db *DB) Log() *DB {
	newDB := *db
	newDB.Debug = true
	return &newDB
}

func (db *DB) Query(target interface{}, query string, args ...interface{}) error {
	return db.QueryContext(context.Background(), target, query, args...)
}

// Query runs a query and fills the received rows or row into the target.
// It is a wrapper method around the
//
func (db *DB) QueryContext(ctx context.Context, target interface{}, query string, args ...interface{}) error {
	var (
		rows    *sql.Rows
		err     error
		query0  string
		newArgs []interface{}
	)

	query0, newArgs, err = db.replaceArgs(query, args...)
	if err != nil {
		return err
	}

	rows, err = db.db.Query(query0, newArgs...)
	if err != nil {
		return err
	}

	switch target.(type) {
	case **sql.Rows:
		reflect.ValueOf(target).Elem().Set(reflect.ValueOf(rows))
		return nil
	}

	defer rows.Close()

	err = Scan(target, rows)
	if err != nil {
		return err
	}

	return nil
}

func (db *DB) Exec(execSql string, args ...interface{}) error {
	return db.ExecContext(context.Background(), execSql, args...)
}

func (db *DB) ExecContext(ctx context.Context, execSql string, args ...interface{}) error {
	if execSql == "" {
		return errors.New("sqlpro error: Query is empty")
	}
	_, err := db.execContext(ctx, -1, execSql, args...)
	return err
}
