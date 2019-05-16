package sqlpro

import (
	"database/sql"
	"log"
	"os"
	"testing"

	_ "github.com/mattn/go-sqlite3"
)

var db *DB

type testRow struct {
	A int64   `db:"a"`
	B string  `db:"b"`
	C string  `db:"c"`
	D float64 `db:"d"`

	ignore string

	A_P *int64   `db:"a_p"`
	B_P *string  `db:"b_p"`
	C_P *string  `db:"c_p"`
	D_P *float64 `db:"d_p"`
}

func cleanup() {
	os.Remove("./test.db")
}

func TestMain(m *testing.M) {

	var (
		err error
	)

	cleanup()

	dbWrap, err := sql.Open("sqlite3", "./test.db")
	if err != nil {
		log.Fatal(err)
	}

	_, err = dbWrap.Exec(`
	CREATE TABLE test(
		a INTEGER PRIMARY KEY AUTOINCREMENT,
		b TEXT,
		c TEXT,
		d REAL
	);
	INSERT INTO test(b) VALUES ('foo');
	INSERT INTO test(b,c,d) VALUES ('bar','other', 1.2345)
	`)

	if err != nil {
		cleanup()
		log.Fatal(err)
	}

	db = NewSqlPro(dbWrap)
	exitCode := m.Run()
	cleanup()
	os.Exit(exitCode)
}

func TestNoPointer(t *testing.T) {
	row := testRow{}

	err := db.SelectRow(row, "SELECT * FROM test LIMIT 1")
	if err == nil {
		t.Errorf("Expected error for passing struct instead of ptr.")
	}
}

func TestNoStruct(t *testing.T) {
	row := 4

	err := db.SelectRow(&row, "SELECT * FROM test LIMIT 1")
	if err == nil {
		t.Errorf("Expected error for passing non struct ptr.")
	}
}

func TestSelectRow(t *testing.T) {

	row := testRow{}
	err := db.SelectRow(&row, "SELECT a, b, c, d FROM test ORDER BY a LIMIT 1 OFFSET 1")

	if err != nil {
		t.Error(err)
	}

	if row.B != "bar" {
		t.Errorf("row.B != 'bar'")
	}

}

func TestSelectRowReal(t *testing.T) {

	row := testRow{}
	err := db.SelectRow(&row, "SELECT a, b, c, d FROM test ORDER BY a LIMIT 1 OFFSET 1")

	if err != nil {
		t.Error(err)
	}

	if row.B != "bar" {
		t.Errorf("row.B != 'bar'")
	}

	if row.D != 1.2345 {
		t.Errorf("row.B != 1.2345")
	}

}

func TestSelectOneRowStd(t *testing.T) {

	row := testRow{}

	rows, err := db.DB.Query("SELECT c, c AS c_p, d AS d_p FROM test ORDER BY a LIMIT 1 OFFSET 1")
	if err != nil {
		t.Error(err)
	}

	rows.Next()
	err = rows.Scan(&row.C, &row.C_P, &row.D_P)
	if err != nil {
		t.Error(err)
	}

}

func TestSelectRowPtr(t *testing.T) {

	row := testRow{}

	s := "henk"
	row.C_P = &s

	err := db.SelectRow(&row, "SELECT a AS a_p, b AS b_p, c AS c_p, d AS d_p FROM test LIMIT 1")

	if err != nil {
		t.Error(err)
	}

	if *row.B_P != "foo" {
		t.Errorf("*row.B_P != 'foo'")
	}

	if *row.A_P != 1 {
		t.Errorf("*row.A_P != 1")
	}

	if row.C_P != nil {
		t.Errorf("row.C_P != nil")
	}

}
