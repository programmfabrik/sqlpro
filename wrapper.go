package sqlpro

import (
	"database/sql"
	"fmt"
	"log"
	"os"
	"reflect"
	"strings"

	"github.com/olekukonko/tablewriter"
)

type DB struct {
	DB               dbWrappable
	Debug            bool
	SingleQuote      rune
	DoubleQuote      rune
	PlaceholderValue rune
	PlaceholderKey   rune
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

type dbWrappable interface {
	Query(query string, args ...interface{}) (*sql.Rows, error)
	Exec(query string, args ...interface{}) (sql.Result, error)
}

// NewSqlPro returns a wrapped database handle providing
// access to the sql pro functions.
func NewWrapper(dbWrap dbWrappable) *DB {
	var (
		db *DB
	)
	db = new(DB)
	db.DB = dbWrap
	db.SingleQuote = '\''
	db.DoubleQuote = '"'
	db.PlaceholderValue = '?'
	db.PlaceholderKey = '@'

	return db
}

func (db *DB) Esc(s string) string {
	return `"` + strings.ReplaceAll(s, `"`, `""`) + `"`
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
		return debugError(fmt.Errorf("%s query: %s", err, query))
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

	if db.Debug {
		// log.Printf("Query: %s Args: %v", query, args)
		db.PrintQuery(query, args...)
	}

	return nil
}

func (db *DB) Exec(execSql string, args ...interface{}) error {
	_, err := db.exec(-1, execSql, args...)
	return err
}

func (db *DB) Delete(execSql string, args ...interface{}) error {
	_, err := db.exec(-1, "DELETE FROM "+execSql, args)
	return err
}

func (db *DB) PrintQuery(query string, args ...interface{}) {
	var (
		rows *sql.Rows
		err  error
	)

	data := make([][]string, 0)

	rows, err = db.DB.Query(query, args...)
	if err != nil {
		log.Println(err)
		return
	}
	cols, _ := rows.Columns()
	defer rows.Close()

	err = Scan(&data, rows)
	if err != nil {
		log.Println(err)
		return
	}

	fmt.Fprintf(os.Stdout, "\n%s\n\n", query)
	table := tablewriter.NewWriter(os.Stdout)
	table.SetHeader(cols)
	table.AppendBulk(data)
	table.Render()

}

func debugError(err error) error {
	log.Printf("sqlpro error: %s", err)
	return err
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
		err = fmt.Errorf("\n\nDatabase Error: %s\n\nSQL:\n %s \nARGS:\n %v\n", err, execSql0, newArgs)
		return 0, debugError(err)
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

	last_insert_id, err := result.LastInsertId()
	if err != nil {
		return 0, debugError(err)
	}
	return last_insert_id, nil
}
