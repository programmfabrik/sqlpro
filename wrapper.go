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

	"errors"

	"github.com/olekukonko/tablewriter"
	"github.com/olekukonko/tablewriter/tw"
	"github.com/yudai/pp"
)

type dbDriver string

// The driver strings must match the driver from the stdlib
const POSTGRES = "postgres"
const SQLITE3 = "sqlite3"

type db struct {
	db                    dbWrappable
	sqlDB                 *sql.DB // this can be <nil>
	driverConn            any     // set in txBeginContext by ExecTX
	sqlTx                 *sql.Tx
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
	driver                dbDriver
	DSN                   string
	isClosed              bool

	txWriteMode bool
	timeFormat  string // if set, format time values in this format (used by sqlite)

	LastError error // This is set to the last error

	txBeforeCommit  []func() error
	txAfterCommit   []func()
	txAfterRollback []func()

	txBeginMtx     *sync.Mutex // used to protect write tx begin for SQLITE3
	txExecQueryMtx *sync.Mutex // used to protect a tx from mutual use during exec or query
}

// DB returns the wrapped sql.DB handle
func (db2 *db) DB() *sql.DB {
	return db2.sqlDB
}

func (db2 *db) String() string {
	return fmt.Sprintf("[%s, %p]", db2.driver, db2)
}

type DebugLevel int

const (
	PANIC      DebugLevel = 1
	ERROR      DebugLevel = 2
	UPDATE     DebugLevel = 4
	INSERT     DebugLevel = 8
	EXEC       DebugLevel = 16
	QUERY      DebugLevel = 32
	QUERY_DUMP DebugLevel = 64
)

type PlaceholderMode int

const (
	DOLLAR   PlaceholderMode = 1
	QUESTION PlaceholderMode = 2
)

type dbWrappable interface {
	Query(query string, args ...any) (*sql.Rows, error)
	QueryContext(ctx context.Context, query string, args ...any) (*sql.Rows, error)
	Exec(query string, args ...any) (sql.Result, error)
	ExecContext(ctx context.Context, query string, args ...any) (sql.Result, error)
}

type Query interface {
	// QueryContext runs the query and scans the result into target: a
	// pointer to a struct, a slice of structs / struct pointers, a scalar
	// or a slice of scalars.
	QueryContext(context.Context, any, string, ...any) error
	// Query works like QueryContext with context.Background().
	Query(any, string, ...any) error
	// Driver returns the database driver the connection runs on.
	Driver() dbDriver
	// EscValue returns s as an escaped SQL string literal (single quoted).
	EscValue(string) string
}

type Exec interface {
	Query

	// ExecContext executes the statement.
	ExecContext(context.Context, string, ...any) error
	// Exec works like ExecContext with context.Background().
	Exec(string, ...any) error
	// ExecContextRowsAffected executes the statement and returns the number
	// of affected rows and the last insert id (if the driver supports it).
	ExecContextRowsAffected(context.Context, string, ...any) (int64, int64, error)

	// Insert inserts one struct (or each row of a slice, one statement per
	// row) into the table and writes the generated primary key back into
	// the row (in slice mode only into an int64 key).
	Insert(string, any) error
	// InsertBulk inserts a slice of structs with one multi-row INSERT
	// (COPY FROM on postgres inside ExecTX). Generated primary keys are NOT
	// read back; use InsertBulkReadbackIdsContext when they are needed.
	InsertBulk(string, any) error
	// InsertBulkContext works like InsertBulk with a context.
	InsertBulkContext(context.Context, string, any) error
	// InsertBulkOnConflictDoNothingContext works like InsertBulkContext but
	// adds ON CONFLICT (cols...) DO NOTHING to the statement.
	InsertBulkOnConflictDoNothingContext(context.Context, string, any, ...string) error
	// InsertBulkReadbackIdsContext works like InsertBulkContext but writes the
	// generated primary keys back into the rows with one INSERT ... RETURNING
	// (so no COPY). The batch needs a single settable auto-assigned integer
	// primary key ("pk,omitempty") and no pre-set key.
	InsertBulkReadbackIdsContext(context.Context, string, any) error
	// InsertContext works like Insert with a context.
	InsertContext(context.Context, string, any) error

	// Save inserts the struct if its primary key is zero, otherwise it
	// updates the row.
	Save(string, any) error
	// SaveContext works like Save with a context.
	SaveContext(context.Context, string, any) error

	// Update updates the table row matching the struct's primary key,
	// setting all non-readonly columns (omitempty columns are skipped
	// when their value is zero).
	Update(string, any) error
	// UpdateContext works like Update with a context.
	UpdateContext(context.Context, string, any) error
	// UpdateBulkContext updates each row of a slice, one statement per row.
	UpdateBulkContext(context.Context, string, any) error
}

type DB interface {
	Query
	Exec
	// Begin starts a read-write transaction.
	Begin() (TX, error)
	// BeginRead starts a read-only transaction.
	BeginRead() (TX, error)
	// BeginContext starts a transaction with the given options.
	BeginContext(context.Context, *sql.TxOptions) (TX, error)
	// Close closes the database connection.
	Close() error
	// IsClosed reports whether Close was called.
	IsClosed() bool
	// Name returns the name of the connected database.
	Name() (string, error)
	// DB returns the wrapped stdlib sql.DB handle.
	DB() *sql.DB
	// Log returns a debug handle which logs every statement; queries are
	// run an extra time to print their result table. Development only.
	Log() DB
	// Version returns the database server version.
	Version() (string, error)
	// ExecTX runs f inside a transaction: commit if f returns nil,
	// rollback otherwise.
	ExecTX(context.Context, func(context.Context) error, *sql.TxOptions) error
}

type TX interface {
	Query
	Exec

	// BeforeCommit registers f to run before the commit; an error from f
	// aborts the commit.
	BeforeCommit(func() error)
	// AfterCommit registers f to run after a successful commit.
	AfterCommit(func())
	// AfterRollback registers f to run after a rollback.
	AfterRollback(func())
	// AfterTransaction registers f to run after the transaction ended,
	// after a successful commit or rollback (not when the underlying
	// commit/rollback itself fails).
	AfterTransaction(func())

	// ActiveTX reports whether the transaction is still open.
	ActiveTX() bool
	// IsWriteMode reports whether the transaction is read-write.
	IsWriteMode() bool

	// Commit commits the transaction.
	Commit() error
	// Rollback rolls the transaction back.
	Rollback() error

	// EscValue returns s as an escaped SQL string literal (single quoted).
	EscValue(string) string
}

// NewSqlPro returns a wrapped database handle providing
// access to the sql pro functions.
func newSqlPro(dbWrap dbWrappable) (dbOut *db) {
	dbOut = &db{}
	dbOut.txBeginMtx = &sync.Mutex{}

	dbOut.db = dbWrap

	// DEFAULTs for sqlite
	dbOut.PlaceholderMode = QUESTION
	dbOut.PlaceholderValue = '?'
	dbOut.PlaceholderEscape = '\\'
	dbOut.PlaceholderKey = '@'
	dbOut.DebugExec = false
	dbOut.MaxPlaceholder = 100
	dbOut.SupportsLastInsertId = true
	dbOut.UseReturningForLastId = false

	return dbOut
}

// Esc escapes s to use as sql name
func (db2 *db) Esc(s string) string {
	return `"` + strings.ReplaceAll(s, `"`, `""`) + `"`
}

// EscValue escapes s to use as sql value
func (db2 *db) EscValue(s string) string {
	return escValue(s)
}

func escValue(s string) string {
	return `'` + strings.ReplaceAll(s, `'`, `''`) + `'`
}

// Version returns the version of the connected database
func (db2 *db) Version() (version string, err error) {
	var selVersion, prefix string
	switch db2.driver {
	case POSTGRES:
		selVersion = "SELECT version()"
	case SQLITE3:
		selVersion = "SELECT sqlite_version()"
		prefix = "Sqlite "
	}
	if selVersion != "" {
		err = db2.Query(&version, selVersion)
		if err != nil {
			return "", fmt.Errorf("reading database version failed: %w", err)
		}
	} else {
		version = "<unsupported driver>"
	}
	return prefix + version, nil
}

// Name returns the name the connected database
func (db2 *db) Name() (name string, err error) {
	var selVersion string
	switch db2.driver {
	case POSTGRES:
		selVersion = "SELECT current_database()"

	case SQLITE3:
		selVersion = `SELECT file FROM pragma_database_list WHERE name = 'main'`
	}
	if selVersion != "" {
		err = db2.Query(&name, selVersion)
		if err != nil {
			return "", fmt.Errorf("reading database version failed: %w", err)
		}
	} else {
		name = "<unsupported driver>"
	}
	return name, nil
}

// Log returns a copy with debug enabled
func (db2 *db) Log() DB {
	newDB := *db2
	newDB.Debug = true
	return &newDB
}

func (db2 *db) Query(target any, query string, args ...any) error {
	return db2.QueryContext(context.Background(), target, query, args...)
}

// QueryContext runs a query and fills the received rows or row into the target.
// It is a wrapper method around the
func (db2 *db) QueryContext(ctx context.Context, target any, query string, args ...any) error {
	var (
		rows    *sql.Rows
		err     error
		query0  string
		newArgs []any
	)

	if db2.txExecQueryMtx != nil {
		db2.txExecQueryMtx.Lock()
		defer db2.txExecQueryMtx.Unlock()
	}

	query0, newArgs, err = db2.replaceArgs(query, args...)
	if err != nil {
		return err
	}

	if (db2.Debug || db2.DebugQuery) && !strings.HasPrefix(query, "INSERT INTO") {
		// log.Printf("Query: %s Args: %v", query, args)
		err = db2.PrintQueryContext(ctx, query, args...)
		if err != nil {
			panic(err)
		}
	}

	// log.Printf("RowMode: %s %v", targetValue.Type().Kind(), rowMode)
	rows, err = db2.db.QueryContext(ctx, query0, newArgs...)
	if err != nil {
		return db2.debugError(db2.sqlError(err, query0, newArgs))
	}

	switch target.(type) {
	case **sql.Rows:
		reflect.ValueOf(target).Elem().Set(reflect.ValueOf(rows))
		return nil
	}

	defer rows.Close()

	err = Scan(target, rows)
	if err != nil {
		return db2.debugError(err)
	}

	return nil
}

func (db2 *db) Exec(execSql string, args ...any) error {
	return db2.ExecContext(context.Background(), execSql, args...)
}

func (db2 *db) ExecContext(ctx context.Context, execSql string, args ...any) error {
	if execSql == "" {
		return db2.debugError(fmt.Errorf("Exec: Empty query"))
	}
	_, _, err := db2.execContext(ctx, execSql, args...)
	return err
}

// ExecContextRowsAffected executes execSql in context ctx and returns the
// number of affected rows and the last insert id (if the driver supports it).
func (db2 *db) ExecContextRowsAffected(ctx context.Context, execSql string, args ...any) (rowsAffected int64, insertID int64, err error) {
	if execSql == "" {
		return 0, 0, db2.debugError(errors.New("Exec: Empty query"))
	}
	return db2.execContext(ctx, execSql, args...)
}

func (db2 *db) PrintQueryContext(ctx context.Context, query string, args ...any) error {
	var (
		rows    *sql.Rows
		err     error
		query0  string
		newArgs []any
	)

	data := make([][]string, 0)

	query0, newArgs, err = db2.replaceArgs(query, args...)

	start := time.Now()
	rows, err = db2.db.QueryContext(ctx, query0, newArgs...)
	if err != nil {
		pp.Println(query0)
		pp.Println(newArgs)
		return db2.sqlError(err, query0, newArgs)
	}
	cols, _ := rows.Columns()
	defer rows.Close()

	err = Scan(&data, rows)
	if err != nil {
		log.Println(err)
		return err
	}

	fmt.Fprint(os.Stdout, db2.sqlDebug(query0, newArgs))
	table := tablewriter.NewWriter(os.Stdout)
	table.Header(cols)
	table.Bulk(data)
	table.Caption(tw.Caption{Text: "Took: " + time.Since(start).String()})
	table.Render()

	return nil
}

func (db2 *db) debugError(err error) error {
	if err == ErrQueryReturnedZeroRows {
		return err
	}
	db2.LastError = err
	if db2.Debug {
		log.Printf("sqlpro error: %s", err)
	}
	return err
}

func (db2 *db) sqlError(err error, sqlS string, args []any) error {
	return fmt.Errorf("Database Error: %s: %w", db2.sqlDebug(sqlS, args), err)
}

func (db2 *db) sqlDebug(sqlS string, args []any) string {
	// if len(sqlS) > 1000 {
	// 	return fmt.Sprintf("SQL:\n %s \nARGS:\n%v\n", sqlS[0:1000], argsToString(args...))
	// }
	return fmt.Sprintf("%s SQL:\n %s \nARGS:\n%v\n", db2, sqlS, argsToString(args...))
}
