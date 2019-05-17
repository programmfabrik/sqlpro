package sqlpro

import (
	"database/sql"
	"fmt"
	"log"
	"os"
	"testing"

	_ "github.com/mattn/go-sqlite3"
)

var db *DB

type testRow struct {
	A int64   `db:"a,pk,omitempty"`
	B string  `db:"b,omitempty"`
	C string  `db:"c,omitempty"`
	D float64 `db:"d,omitempty"`

	ignore string

	A_P *int64   `db:"a_p,omitempty"`
	B_P *string  `db:"b_p,omitempty"`
	C_P *string  `db:"c_p,omitempty"`
	D_P *float64 `db:"d_p,omitempty"`
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
	`)

	if err != nil {
		cleanup()
		log.Fatal(err)
	}

	db = NewWrapper(dbWrap)
	exitCode := m.Run()
	cleanup()
	os.Exit(exitCode)
}

func TestInsert(t *testing.T) {
	data := []*testRow{
		&testRow{
			B: "foo",
		},
		&testRow{
			B: "bar",
			C: "other",
			D: 1.2345,
		},
	}
	err := db.Insert("test", data)
	if err != nil {
		t.Error(err)
	}
}

func TestInsertMany(t *testing.T) {
	for i := 0; i < 1000; i++ {
		tr := testRow{
			B: fmt.Sprintf("row %d", i+1),
			D: float64(i + 1),
		}
		err := db.Insert("test", tr)
		if err != nil {
			t.Error(err)
		}
	}
}

func TestNoPointer(t *testing.T) {
	row := testRow{}

	err := db.Query(row, "SELECT * FROM test LIMIT 1")
	if err == nil {
		t.Errorf("Expected error for passing struct instead of ptr.")
	}
}

func TestNoStruct(t *testing.T) {
	var i int64

	err := db.Query(&i, "SELECT * FROM test ORDER BY a LIMIT 1")
	if err != nil {
		t.Error(err)
	}
	if i != 1 {
		t.Errorf("Expected i == 1.")
	}
}

func TestQuery(t *testing.T) {

	row := testRow{}
	err := db.Query(&row, "SELECT a, b, c, d FROM test ORDER BY a LIMIT 1 OFFSET 1")

	if err != nil {
		t.Error(err)
	}

	if row.B != "bar" {
		t.Errorf("row.B != 'bar'")
	}

}

func TestQueryReal(t *testing.T) {

	row := testRow{}
	err := db.Query(&row, "SELECT a, b, c, d FROM test ORDER BY a LIMIT 1 OFFSET 1")

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

func TestQueryOneRowStd(t *testing.T) {

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

func TestQueryPtr(t *testing.T) {

	row := testRow{}

	// this needs to be set <nil> by sqlpro
	s := "henk"
	row.C_P = &s

	err := db.Query(&row, "SELECT a AS a_p, b AS b_p, c AS c_p, d AS d_p FROM test ORDER BY a LIMIT 1")

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

	if row.D_P != nil {
		t.Errorf("row.D_P != nil")
	}

}

func TestQueryAll(t *testing.T) {
	rows := make([]testRow, 0)
	err := db.Query(&rows, "SELECT * FROM test")
	if err != nil {
		t.Error(err)
	}
}

func TestQueryAllPtr(t *testing.T) {
	rows := make([]*testRow, 0)
	err := db.Query(&rows, "SELECT * FROM test")
	if err != nil {
		t.Error(err)
	}
}

func TestQueryAllInt64(t *testing.T) {
	rows := make([]int64, 0)
	err := db.Query(&rows, "SELECT a FROM test")
	if err != nil {
		t.Error(err)
	}
}

func TestQueryAllInt64Ptr(t *testing.T) {
	rows := make([]*int64, 0)
	err := db.Query(&rows, "SELECT a FROM test")
	if err != nil {
		t.Error(err)
	}
}

func TestQueryAllIntPtr(t *testing.T) {
	rows := make([]*int, 0)
	err := db.Query(&rows, "SELECT a FROM test")
	if err != nil {
		t.Error(err)
	}
	// litter.Dump(rows)
}
func TestQueryAllFloat64Ptr(t *testing.T) {
	rows := make([]*float64, 0)
	err := db.Query(&rows, "SELECT d FROM test ORDER BY a")
	if err != nil {
		t.Error(err)
	}
	if rows[0] != nil {
		t.Errorf("First d needs to be <nil>.")
	}
	// litter.Dump(rows)
}

func TestCountAll(t *testing.T) {
	var i *int64
	err := db.Query(&i, "SELECT count(*) FROM test")
	if err != nil {
		t.Error(err)
	}
	if *i != 1002 {
		t.Errorf("count needs to be 2 but is: %d.", *i)
	}
}
