package sqlpro

import (
	"database/sql"
	"fmt"
	"log"
	"os"
	"reflect"
	"strings"

	"github.com/olekukonko/tablewriter"
	"github.com/programmfabrik/fylr/config"
	"golang.org/x/xerrors"
)

type DB struct {
	DB                    dbWrappable
	Debug                 bool
	PlaceholderMode       PlaceholderMode
	PlaceholderEscape     rune
	PlaceholderValue      rune
	PlaceholderKey        rune
	MaxPlaceholder        int
	UseReturningForLastId bool
	SupportsLastInsertId  bool
	driver, dsn           string
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
	Exec(query string, args ...interface{}) (sql.Result, error)
	Close() error
}

// NewSqlPro returns a wrapped database handle providing
// access to the sql pro functions.
func NewWrapper(dbWrap dbWrappable) *DB {
	var (
		db *DB
	)
	db = new(DB)
	db.DB = dbWrap

	// DEFAULTs for sqlite
	db.PlaceholderMode = QUESTION
	db.PlaceholderValue = '?'
	db.PlaceholderEscape = '\\'
	db.PlaceholderKey = '@'
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

// Query runs a query and fills the received rows or row into the target.
// It is a wrapper method around the
//
func (db *DB) Query(target interface{}, query string, args ...interface{}) error {
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

	rows, err = db.DB.Query(query0, newArgs...)
	if err != nil {
		return debugError(sqlError(err, query0, newArgs))
	}

	switch target.(type) {
	case **sql.Rows:
		reflect.ValueOf(target).Elem().Set(reflect.ValueOf(rows))
		return nil
	}

	defer rows.Close()

	err = Scan(target, rows)
	if err != nil {
		return debugError(err)
	}

	if db.Debug && !strings.HasPrefix(query, "INSERT INTO") {
		// log.Printf("Query: %s Args: %v", query, args)
		err = db.PrintQuery(query, args...)
		if err != nil {
			panic(err)
		}
	}

	return nil
}

func (db *DB) Exec(execSql string, args ...interface{}) error {
	_, err := db.exec(-1, execSql, args...)
	return err
}

func (db *DB) PrintQuery(query string, args ...interface{}) error {
	var (
		rows    *sql.Rows
		err     error
		query0  string
		newArgs []interface{}
	)

	data := make([][]string, 0)

	query0, newArgs, err = db.replaceArgs(query, args...)

	rows, err = db.DB.Query(query0, newArgs...)
	if err != nil {
		return sqlError(err, query0, newArgs)
	}
	cols, _ := rows.Columns()
	defer rows.Close()

	err = Scan(&data, rows)
	if err != nil {
		log.Println(err)
		return err
	}

	fmt.Fprintf(os.Stdout, "\n%s\n\n", query)
	table := tablewriter.NewWriter(os.Stdout)
	table.SetHeader(cols)
	table.AppendBulk(data)
	table.Render()

	return nil
}

func (db *DB) Close() error {
	log.Printf("sqlpro.Close: %p %s %s", db.DB, db.driver, db.dsn)
	return db.DB.Close()
}

// Open opens a database connection and returns an sqlpro wrap handle
func Open(driver, dsn string) (*DB, error) {

	conn, err := sql.Open(driver, dsn)
	if err != nil {
		return nil, err
	}

	// conn.SetMaxOpenConns(1)

	err = conn.Ping()
	if err != nil {
		conn.Close()
		return nil, err
	}

	wrapper := NewWrapper(conn)
	// wrapper.Debug = true

	wrapper.dsn = dsn
	wrapper.driver = driver

	switch config.FylrConfig.Server.DB.Driver {
	case "postgres":
		wrapper.PlaceholderMode = DOLLAR
		wrapper.UseReturningForLastId = true
		wrapper.SupportsLastInsertId = false
	case "sqlite3":
	default:
		return nil, xerrors.Errorf("sqlpro.Open: Unsupported driver '%s'.", driver)
	}

	log.Printf("sqlpro.Open: %p %s %s", wrapper.DB, driver, dsn)
	return wrapper, nil
}

func debugError(err error) error {
	log.Printf("sqlpro error: %s", err)
	return err
}

func sqlError(err error, sqlS string, args []interface{}) error {
	return xerrors.Errorf("Database Error: %s\n\nSQL:\n %s \nARGS:\n%v\n", err, sqlS, argsToString(args...))
}

// exec wraps DB.Exec and automatically checks the number of Affected rows
// if expRows == -1, the check is skipped
func (db *DB) exec(expRows int64, execSql string, args ...interface{}) (int64, error) {
	var (
		execSql0 string
		err      error
		newArgs  []interface{}
	)

	if db.Debug {
		log.Printf("SQL: %s\nARGS:\n%s", execSql, argsToString(args...))
	}

	execSql0, newArgs, err = db.replaceArgs(execSql, args...)
	if err != nil {
		return 0, err
	}
	result, err := db.DB.Exec(execSql0, newArgs...)
	if err != nil {
		return 0, debugError(sqlError(err, execSql0, newArgs))
	}
	row_count, err := result.RowsAffected()
	if err != nil {
		return 0, err
	}

	if expRows == -1 {
		return row_count, nil
	}

	if row_count != expRows {
		return 0, debugError(fmt.Errorf("Exec affected only %d out of %d.", row_count, expRows))
	}

	if !db.SupportsLastInsertId {
		return 0, nil
	}

	last_insert_id, err := result.LastInsertId()
	if err != nil {
		return 0, debugError(err)
	}
	return last_insert_id, nil
}
