package sqlpro

import (
	"context"
	"database/sql"
	"fmt"
	"log"
	"os"
	"reflect"
	"strings"
	"sync"
	"time"

	"github.com/olekukonko/tablewriter"
	"github.com/pkg/errors"
	"github.com/programmfabrik/golib"
	"github.com/yudai/pp"
)

type dbDriver string

// The driver strings must match the driver from the stdlib
const POSTGRES = "postgres"
const SQLITE3 = "sqlite3"

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
	Driver                dbDriver
	DSN                   string
	isClosed              bool

	txWriteMode bool

	LastError error // This is set to the last error

	txAfterCommit   []func()
	txAfterRollback []func()

	txBeginMtx *sync.Mutex // used to protect write tx begin for SQLITE3
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

type DebugLevel int

const (
	PANIC      DebugLevel = 1
	ERROR                 = 2
	UPDATE                = 4
	INSERT                = 8
	EXEC                  = 16
	QUERY                 = 32
	QUERY_DUMP            = 64
)

type PlaceholderMode int

const (
	DOLLAR   PlaceholderMode = 1
	QUESTION                 = 2
)

type dbWrappable interface {
	Query(query string, args ...interface{}) (*sql.Rows, error)
	QueryContext(ctx context.Context, query string, args ...interface{}) (*sql.Rows, error)
	Exec(query string, args ...interface{}) (sql.Result, error)
	ExecContext(ctx context.Context, query string, args ...interface{}) (sql.Result, error)
}

// NewSqlPro returns a wrapped database handle providing
// access to the sql pro functions.
func New(dbWrap dbWrappable) *DB {
	var (
		db *DB
	)
	db = new(DB)

	db.txBeginMtx = &sync.Mutex{}
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

// Version returns the version of the connected database
func (db *DB) Version() (version string, err error) {
	var selVersion, prefix string
	switch db.Driver {
	case POSTGRES:
		selVersion = "SELECT version()"
	case SQLITE3:
		selVersion = "SELECT sqlite_version()"
		prefix = "Sqlite "
	}
	if selVersion != "" {
		err = db.Query(&version, selVersion)
		if err != nil {
			return "", fmt.Errorf("reading database version failed: %w", err)
		}
	} else {
		version = "<unsupported driver>"
	}
	return prefix + version, nil
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

	// log.Printf("RowMode: %s %v", targetValue.Type().Kind(), rowMode)
	rows, err = db.db.QueryContext(ctx, query0, newArgs...)
	if err != nil {
		return db.debugError(db.sqlError(err, query0, newArgs))
	}

	switch target.(type) {
	case **sql.Rows:
		reflect.ValueOf(target).Elem().Set(reflect.ValueOf(rows))
		return nil
	}

	defer rows.Close()

	err = Scan(target, rows)
	if err != nil {
		return db.debugError(err)
	}

	if (db.Debug || db.DebugQuery) && !strings.HasPrefix(query, "INSERT INTO") {
		// log.Printf("Query: %s Args: %v", query, args)
		err = db.PrintQueryContext(ctx, query, args...)
		if err != nil {
			panic(err)
		}
	}

	return nil
}

func (db *DB) Exec(execSql string, args ...interface{}) error {
	return db.ExecContext(context.Background(), execSql, args...)
}

func (db *DB) ExecContext(ctx context.Context, execSql string, args ...interface{}) error {
	if execSql == "" {
		return db.debugError(errors.New("Exec: Empty query"))
	}
	_, _, err := db.execContext(ctx, execSql, args...)
	return err
}

// ExecContextExp executes execSql in context ctx. If the number of rows affected
// doesn't match expRows, an error is returned.
func (db *DB) ExecContextRowsAffected(ctx context.Context, execSql string, args ...interface{}) (int64, int64, error) {
	if execSql == "" {
		return 0, 0, db.debugError(errors.New("Exec: Empty query"))
	}
	return db.execContext(ctx, execSql, args...)
}

func (db *DB) PrintQueryContext(ctx context.Context, query string, args ...interface{}) error {
	var (
		rows    *sql.Rows
		err     error
		query0  string
		newArgs []interface{}
	)

	data := make([][]string, 0)

	query0, newArgs, err = db.replaceArgs(query, args...)

	start := time.Now()
	rows, err = db.db.QueryContext(ctx, query0, newArgs...)
	if err != nil {
		pp.Println(query0)
		pp.Println(newArgs)
		return db.sqlError(err, query0, newArgs)
	}
	cols, _ := rows.Columns()
	defer rows.Close()

	err = Scan(&data, rows)
	if err != nil {
		log.Println(err)
		return err
	}

	fmt.Fprint(os.Stdout, db.sqlDebug(query0, newArgs))
	table := tablewriter.NewWriter(os.Stdout)
	table.SetHeader(cols)
	table.AppendBulk(data)
	table.SetCaption(true, "Took: "+time.Since(start).String())
	table.Render()

	return nil
}

func (db *DB) debugError(err error) error {
	if err == ErrQueryReturnedZeroRows {
		return err
	}
	db.LastError = err
	if db.Debug {
		log.Printf("sqlpro error: %s", err)
	}
	return err
}

func (db *DB) sqlError(err error, sqlS string, args []interface{}) error {
	return errors.Wrapf(err, "Database Error: %s", db.sqlDebug(sqlS, args))
}

func (db *DB) sqlDebug(sqlS string, args []interface{}) string {
	// if len(sqlS) > 1000 {
	// 	return fmt.Sprintf("SQL:\n %s \nARGS:\n%v\n", sqlS[0:1000], argsToString(args...))
	// }
	return fmt.Sprintf("%s SQL:\n %s \nARGS:\n%v\n", db, golib.CutStr(sqlS, 2000, "..."), argsToString(args...))
}
