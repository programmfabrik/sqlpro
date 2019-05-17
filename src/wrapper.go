package sqlpro

import (
	"database/sql"
)

type DB struct {
	DB  *sql.DB
	Esc func(string) string // escape function
}

// NewSqlPro returns a wrapped database handle providing
// access to the sql pro functions.
func NewWrapper(dbWrap *sql.DB) *DB {
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
		return err
	}

	err = Scan(target, rows)
	if err != nil {
		return err
	}
	return nil
}
