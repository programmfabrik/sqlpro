package sqlpro

import (
	"database/sql"
	"database/sql/driver"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"testing"
	"time"

	"github.com/kr/pretty"
	_ "github.com/mattn/go-sqlite3"
)

var db *DB

type jsonStore struct {
	Field  string `db:"field"`
	Field2 string `db:"field2"`
}

func (js jsonStore) Value() (driver.Value, error) {
	if js.Field == "" && js.Field2 == "" {
		return nil, nil
	}
	return json.Marshal(js)
}

func (js *jsonStore) Scan(value interface{}) error {
	switch v := value.(type) {
	case nil:
		return nil
	case []byte:
		return json.Unmarshal(v, &js)
	default:
		return fmt.Errorf("jsonStore: Unable to Scan type %T", value)
	}
}

type testRow struct {
	A int64      `db:"a,pk,omitempty"`
	B string     `db:"b,omitempty"`
	C string     `db:"c,notnull"`
	D float64    `db:"d,omitempty"`
	E *time.Time `db:"e"`
	F jsonStore  `db:"f"`

	ignore string
}

type testRowPtr struct {
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
		d REAL,
		e DATETIME,
		f TEXT,
		"""" TEXT
	);
	`)

	if err != nil {
		cleanup()
		log.Fatal(err)
	}

	db = NewWrapper(dbWrap)
	db.Debug = false

	exitCode := m.Run()
	cleanup()
	os.Exit(exitCode)
}

func TestInsertSliceStructPtr(t *testing.T) {
	var (
		err  error
		now  time.Time
		now2 time.Time
	)

	now = time.Now()

	data := []*testRow{
		&testRow{
			B: "fooUPDATEME",
		},
		&testRow{
			B: "bar",
			C: "other",
			D: 1.2345,
			E: &now,
			F: jsonStore{"Henk", "Torsten"},
		},
		&testRow{
			B: "torsten",
			C: "other",
			D: 1.2345,
		},
	}

	// db.DebugNext = true

	err = db.Insert("test", data)
	if err != nil {
		t.Error(err)
	}

	for idx, tr := range data {
		if tr.A <= 0 {
			t.Errorf("data[%d].A needs to be set (pk).", idx)
		}
	}

	err = db.Query(&now2, "SELECT E FROM test WHERE C = 'other'")
	if err != nil {
		t.Error(err)
	}

	pretty.Println(now2)
}

func TestInsertSliceStruct(t *testing.T) {
	data := []testRow{
		testRow{
			B: "foo4",
		},
		testRow{
			B: "bar5",
			C: "other",
			D: 1.2345,
		},
	}

	// db.DebugNext = true
	err := db.Insert("test", data)
	if err != nil {
		t.Error(err)
	}

	for idx, tr := range data {
		if tr.A <= 0 {
			t.Errorf("data[%d].A needs to be set (pk).", idx)
		}
	}
}

func TestInsertStructPtr(t *testing.T) {

	tr := testRow{B: "foo2"}

	err := db.Insert("test", &tr)
	if err != nil {
		t.Error(err)
	}
	if tr.A <= 0 {
		t.Errorf("data[0].A needs to be set (pk).")
	}
}
func TestInsertStruct(t *testing.T) {

	tr := testRow{B: "foo3"}
	err := db.Insert("test", tr)
	if err == nil {
		t.Error("Insert must not accept struct.")
	}
}

func TestUpdate(t *testing.T) {
	tr := &testRow{
		A: 1,
		B: "foo",
	}
	// db.DebugNext = true
	err := db.Update("test", tr)
	if err != nil {
		t.Error(err)
	}
}

func TestUpdateMany(t *testing.T) {
	trs := []*testRow{
		&testRow{
			A: 1,
			B: "foo",
		},
		&testRow{
			A: 3,
			B: "torsten2",
		},
	}

	err := db.Update("test", trs)
	if err != nil {
		t.Error(err)
	}
}

func TestSaveMany(t *testing.T) {
	trs := []*testRow{
		&testRow{
			B: "henk",
		},
		&testRow{
			A: 3,
			B: "torsten3",
		},
	}

	err := db.Save("test", trs)
	if err != nil {
		t.Error(err)
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

func TestStandard(t *testing.T) {
	var (
		err   error
		json0 jsonStore
		json1 string
	)

	row := testRowPtr{}

	s := jsonStore{"Henk", "Torsten"}

	_, err = db.DB.Exec("UPDATE test SET f = ? WHERE a = 2", s)
	if err != nil {
		t.Error(err)
	}

	rows, err := db.DB.Query("SELECT b AS b_p, c AS c_p, d AS d_p, f, f FROM test ORDER BY a LIMIT 1 OFFSET 1")
	if err != nil {
		t.Error(err)
	}

	defer rows.Close()

	rows.Next()
	err = rows.Scan(&row.B_P, &row.C_P, &row.D_P, &json0, &json1)
	if err != nil {
		t.Error(err)
	}
	if json0.Field != "Henk" || json0.Field2 != "Torsten" {
		t.Errorf("Field must be Henk and Torsten.")
	}

}

func TestQueryPtr(t *testing.T) {

	row := testRowPtr{}

	// this needs to be set <nil> by sqlpro
	s := "henk"
	row.C_P = &s

	err := db.Query(&row, "SELECT a AS a_p, b AS b_p, c AS c_p, d AS d_p FROM test ORDER BY a LIMIT 1")

	if err != nil {
		t.Error(err)
	}

	if row.B_P == nil || *row.B_P != "foo" {
		t.Errorf("*row.B_P != 'foo'")
	}

	if row.A_P == nil || *row.A_P != 1 {
		t.Errorf("*row.A_P != 1")
	}

	if row.C_P == nil || *row.C_P != "" {
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
	if len(rows) == 0 || rows[0] != nil {
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
	if i == nil || *i <= 0 {
		t.Errorf("count needs to be > 0: %v.", i)
	}
}

func TestCountUint(t *testing.T) {
	var (
		i   uint64
		i2  *uint64
		err error
	)

	err = db.Query(&i, "SELECT count(*) FROM test")
	if err != nil {
		t.Error(err)
	}
	if i <= 0 {
		t.Errorf("count needs to be > 0: %v.", i)
	}
	err = db.Query(&i2, "SELECT count(*) FROM test")
	if err != nil {
		t.Error(err)
	}
	if i2 == nil || *i2 <= 0 {
		t.Errorf("count needs to be > 0: %v.", *i2)
	}
}

func TestSliceStringPtr(t *testing.T) {
	var (
		s   [][]*string
		err error
	)

	err = db.Query(&s, "SELECT * FROM test")
	if err != nil {
		t.Error(err)
	}
}

func TestSave(t *testing.T) {
	var (
		tr  testRow
		err error
	)
	tr = testRow{
		B: "foo_save",
	}

	err = db.Save("test", &tr)
	if err != nil {
		t.Error(err)
	}

	err = db.Save("test", &tr)
	if err != nil {
		t.Error(err)
	}

}

func TestSliceString(t *testing.T) {
	var (
		s   [][]string
		err error
	)

	err = db.Query(&s, "SELECT * FROM test")
	if err != nil {
		t.Error(err)
	}
}

func TestDump(t *testing.T) {
	db.PrintQuery("SELECT * FROM test")
}

func ATestInsertMany(t *testing.T) {
	for i := 0; i < 1000; i++ {
		tr := testRow{
			B: fmt.Sprintf("row %d", i+1),
			D: float64(i + 1),
		}
		err := db.Insert("test", &tr)
		if err != nil {
			t.Error(err)
		}
	}
}

func ATestInsertBulk(t *testing.T) {
	rows := make([]*testRow, 0)
	for i := 0; i < 1000; i++ {
		tr := &testRow{
			B: fmt.Sprintf("row %d", i+1),
			D: float64(i + 1),
		}
		rows = append(rows, tr)
	}

	err := db.InsertBulk("test", rows)
	if err != nil {
		t.Error(err)
	}
}

func TestDelete(t *testing.T) {
	err := db.Exec("DELETE FROM test WHERE id IN ?", []int64{-1, -2, -3})
	if err != nil {
		t.Error(err)
	}
}

func TestQueryIntSlice(t *testing.T) {
	err := db.Exec("SELECT * FROM test WHERE id IN ?", []int64{-1, -2, -3})
	if err != nil {
		t.Error(err)
	}
}

func TestReplaceArgs(t *testing.T) {

	int_args := []int64{1, 3, 4, 5}
	string_args := []string{"a", "b", "c"}

	type test struct {
		sql    string
		args   []interface{}
		expSql string
		expErr bool
	}

	type ica []interface{}

	tests := []test{
		// sql, args, expected, err?
		test{"ID IN ?", ica{int_args}, "ID IN (1,3,4,5)", false},
		test{"ID IN '?'", ica{}, "ID IN '?'", false},
		test{"ID = ?", ica{"hen'k"}, "ID = 'hen''k'", false},
		test{"ID = ?", ica{5}, "ID = 5", false},
		test{"ID IN '''", ica{}, "", true},
		test{"ID IN '?'''", ica{}, "ID IN '?'''", false},
		test{"ID IN '?''' WHERE ?", ica{int_args}, "ID IN '?''' WHERE (1,3,4,5)", false},
		test{"ID IN ?", ica{string_args}, "ID IN ('a','b','c')", false},
	}

	for _, te := range tests {
		sql, err := db.replaceArgs(te.sql, te.args...)
		if err != nil {
			if te.expErr {
				continue
			}
			t.Error(err)
		} else {
			if te.expErr {
				t.Errorf("Error expected for: %s", te.sql)
			}
		}
		if sql != te.expSql {
			t.Errorf("Replace not matching: %s, exp: %s", sql, te.expSql)
		}
	}
}
