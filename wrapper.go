package sqlpro

import (
	"database/sql"
	"fmt"
	"log"
	"os"

	"github.com/olekukonko/tablewriter"
)

type DB struct {
	DB        dbWrappable
	Esc       func(string) string // escape function
	DebugNext bool
	Debug     bool
}

type DebugLevel int

const (
	ERROR      DebugLevel = 1
	UPDATE                = 1
	INSERT                = 2
	EXEC                  = 4
	QUERY                 = 8
	QUERY_DUMP            = 16
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
	db.Esc = func(s string) string {
		return `"` + s + `"`
	}
	return db
}

// Query runs a query and fills the received rows or row into the target.
// It is a wrapper method around the
//
func (db *DB) Query(target interface{}, query string, args ...interface{}) error {
	var (
		rows *sql.Rows
		err  error
	)

	// log.Printf("RowMode: %s %v", targetValue.Type().Kind(), rowMode)

	rows, err = db.DB.Query(query, args...)
	if err != nil {

		return debugError(fmt.Errorf("%s query: %s", err, query))
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
	db.DebugNext = false
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
	if db.DebugNext || db.Debug {
		log.Printf("SQL: %s ARGS: %v", execSql, args)
	}
	result, err := db.DB.Exec(execSql, args...)
	if err != nil {
		if !db.DebugNext {
			log.Printf("\n\nDatabase Error: %s\n\nSQL:\n%s \n\nARGS:\n%v", err, execSql, args)
		}
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
