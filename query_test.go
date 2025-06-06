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

	sqlite3 "github.com/mattn/go-sqlite3"
	"github.com/stretchr/testify/assert"
)

func init() {
	sqlite3.SQLiteTimestampFormats[0] = time.RFC3339Nano
}

func TestMain(m *testing.M) {

	var err error

	cleanup()

	db, err = Open("sqlite3", "./test.db?_foreign_keys=1&_journal=wal&_busy_timeout=1000")
	if err != nil {
		log.Fatal(err)
	}

	var v string
	db.Log().Query(&v, "SELECT sqlite_version()")

	err = db.Exec(`
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

	exitCode := m.Run()
	cleanup()
	os.Exit(exitCode)
}

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
		if len(v) == 0 {
			return nil
		}
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

type myStruct struct {
	A string `json:"a"`
	B string `json:"b"`
}

type testRowJson struct {
	A int64    `db:"a,pk,omitempty"`
	B string   `db:"b"`
	F myStruct `db:"f,json"`
}

type testRowJsonPtr struct {
	A int64     `db:"a,pk,omitempty"`
	B *string   `db:"b"`
	F *myStruct `db:"f,json"`
}

type testRowUint8 struct {
	A int64           `db:"a,pk,omitempty"`
	F json.RawMessage `db:"f"`
}

type testRowUint8Ptr struct {
	A int64            `db:"a,pk,omitempty"`
	F *json.RawMessage `db:"f"`
}

func cleanup() {
	os.Remove("./test.db")
}

func TestInsertSliceStructPtr(t *testing.T) {
	var (
		err      error
		now      time.Time
		readBack testRow
	)

	now = time.Now()

	data := []*testRow{
		{
			B: "fooUPDATEME",
			F: jsonStore{"Yo", "Mama"},
		},
		{
			B: "bar",
			C: "other",
			D: 1.2345,
			E: &now,
			F: jsonStore{"Henk", "Torsten"},
		},
		{
			B: "torsten",
			C: "other",
			D: 1.2345,
		},
	}

	err = db.Insert("test", data)
	if err != nil {
		t.Error(err)
	}

	for idx, tr := range data {
		if tr.A <= 0 {
			t.Errorf("data[%d].A needs to be set (pk).", idx)
		}
	}

	readBack = testRow{}

	err = db.Query(&readBack, "SELECT e FROM test WHERE E IS NOT NULL LIMIT 1")
	if err != nil {
		t.Error(err)
	}

	if readBack.E == nil || !readBack.E.Equal(now) {
		t.Errorf("Time e is <nil> or wrong: %s", readBack.E)
	}

	// db.PrintQuery("SELECT * FROM test WHERE c = 'other'")
	// pretty.Println(now2)
}

func TestInsertSliceStruct(t *testing.T) {
	data := []testRow{
		{
			B: "foo4",
		},
		{
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
	if err != nil {
		t.Error(err)
	}
}

func TestTime(t *testing.T) {

	now := time.Now()

	type timeStruct struct {
		B *time.Time `db:"b"`
		C string     `db:"c"`
	}

	type timeStruct2 struct {
		B time.Time `db:"b"`
		C string    `db:"c"`
	}

	tr := timeStruct{B: &now, C: "timetest"}

	err := db.Insert("test", tr)
	if !assert.NoError(t, err) {
		return
	}

	// timeStr := timeStruct{}
	// err = db.Query(&timeStr, "SELECT b FROM test WHERE c='timetest'")
	// if !assert.NoError(t, err) {
	// 	return
	// }
	// assert.Equal(t, now.Format(time.RFC3339Nano), timeStr.B.Format(time.RFC3339Nano))

	// timeStr2 := timeStruct2{}
	// err = db.Query(&timeStr2, "SELECT b FROM test WHERE c='timetest'")
	// if !assert.NoError(t, err) {
	// 	return
	// }
	// assert.Equal(t, now.Format(time.RFC3339Nano), timeStr2.B.Format(time.RFC3339Nano))

	time1 := &time.Time{}
	err = db.Query(&time1, "SELECT b FROM test WHERE c='timetest'")
	if !assert.NoError(t, err) {
		return
	}
	assert.Equal(t, now.Format(time.RFC3339Nano), time1.Format(time.RFC3339Nano))

	time2 := &time.Time{}
	err = db.Query(&time2, "SELECT b FROM test WHERE c='timetest'")
	if !assert.NoError(t, err) {
		return
	}
	assert.Equal(t, now.Format(time.RFC3339Nano), time2.Format(time.RFC3339Nano))

	time3 := time.Time{}
	err = db.Query(&time3, "SELECT b FROM test WHERE c='timetest'")
	if !assert.NoError(t, err) {
		return
	}
	assert.Equal(t, now.Format(time.RFC3339Nano), time3.Format(time.RFC3339Nano))

}

func TestUpdate(t *testing.T) {
	tr := &testRow{
		A: 1,
		B: "foo",
	}
	err := db.Update("test", tr)
	if err != nil {
		t.Error(err)
	}
}

func TestUpdateMany(t *testing.T) {
	trs := []*testRow{
		{
			A: 1,
			B: "foo",
		},
		{
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
		{
			B: "henk",
		},
		{
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

	defer func() {
		r := recover()
		if r == nil {
			// no panic -> wrong
			t.Errorf("Expected error for passing struct instead of ptr.")
		}
	}()

	db.Query(row, "SELECT * FROM test LIMIT 1")
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

func TestQueryStruct(t *testing.T) {
	row := testRow{}
	db.MaxPlaceholder = 1
	err := db.Query(&row, "SELECT * FROM test WHERE a IN ? LIMIT 1", []int64{1, 2, 3, 4, 5, 6, 7, 8})
	if err != nil {
		t.Error(err)
	}
	err = db.Query(&row, "SELECT * FROM test WHERE b IN ? LIMIT 1", []string{"henk", "horst", "torsten"})
	if err != nil {
		t.Error(err)
	}
}

func TestQueryStruct2(t *testing.T) {
	defer func() {
		r := recover()
		if r == nil {
			t.Errorf("Expected a panic.")
		}
	}()

	row := testRow{}
	db.Query(row, "SELECT * FROM test WHERE A IN ? LIMIT 1", []int64{1, 2, 3, 4, 5, 6, 7, 8})
}

func TestStandard(t *testing.T) {
	var (
		err   error
		json0 jsonStore
		json1 string
	)

	row := testRowPtr{}

	s := jsonStore{"Henk", "Torsten"}

	_, err = db.db.Exec("UPDATE test SET f = ? WHERE a = 2", s)
	if err != nil {
		t.Error(err)
	}

	rows, err := db.db.Query("SELECT b AS b_p, c AS c_p, d AS d_p, f, f FROM test ORDER BY a LIMIT 1 OFFSET 1")
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
	var rows []testRow
	err := db.Query(&rows, "SELECT * FROM test")
	if err != nil {
		t.Error(err)
	}
	if len(rows) == 0 {
		t.Errorf("0 rows.")
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
	var rows []*float64
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

func TestInterfaceSliceSave(t *testing.T) {
	var (
		tr  testRow
		err error
	)
	tr = testRow{
		B: "foo_save",
	}

	i := []interface{}{tr}

	err = db.Save("test", &i)
	if err != nil {
		t.Error(err)
	}

}

func TestInterfaceSlicePtrSave(t *testing.T) {
	var (
		tr  testRow
		err error
	)
	tr = testRow{
		B: "foo_save",
	}

	i := []interface{}{&tr}

	err = db.Save("test", &i)
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

func TestInsertMany(t *testing.T) {
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

func TestInsertBulk(t *testing.T) {
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
	err := db.Exec("DELETE FROM test WHERE a IN ?", []int64{-1, -2, -3})
	if err != nil {
		t.Error(err)
	}
}

func TestQueryIntStruct(t *testing.T) {
	var dummy int64

	err := db.Query(&dummy, "SELECT * FROM test WHERE a IN ?", []int64{-1, -2, -3})
	if err == nil {
		t.Errorf("Expected ErrQueryReturnedZeroRows.")
	}

	// Make sure the error is not wrapped
	if !assert.Equal(t, ErrQueryReturnedZeroRows, err) {
		return
	}
}

func TestQueryIntSlice(t *testing.T) {
	var dummy []int64

	err := db.Query(&dummy, "SELECT * FROM test WHERE a IN ?", []int64{-1, -2, -3})
	if err != nil {
		t.Error(err)
	}
	if len(dummy) != 0 {
		t.Errorf("int slice must not contain entries.")
	}
}

func TestQuerySqlRows(t *testing.T) {
	var (
		err  error
		rows *sql.Rows
		a    int64
		idx  int64
	)
	err = db.Query(&rows, "SELECT a FROM test")
	if err != nil {
		t.Error(err)
	}
	if rows == nil {
		t.Errorf("Rows == <nil>.")
	}

	for rows.Next() {
		err = rows.Scan(&a)
		if err != nil {
			t.Error(err)
		}
		if a == 0 {
			t.Errorf("Scan must return > 0 integer.")
		}
		idx++
	}

	err = rows.Close()
	if err != nil {
		t.Error(err)
	}
	if idx == 0 {
		t.Errorf("No rows received.")
	}

}

func TestQuerySqlRowsNoPtrPtr(t *testing.T) {
	var (
		rows *sql.Rows
	)

	defer func() {
		r := recover()
		if r == nil {
			t.Errorf("Expected a panic.")
		}
	}()

	db.Query(rows, "SELECT * FROM test")
}

func TestJson(t *testing.T) {
	var (
		err    error
		tr     testRowJson
		trPtr  testRowJsonPtr
		trPtr2 testRowJsonPtr
		tr2    []*testRowJson
	)
	jt := "JsonTest"

	tr = testRowJson{B: jt, F: myStruct{A: "JsonTest", B: "Torsten"}}
	err = db.Insert("test", &tr)
	if err != nil {
		t.Error(err)
	}
	tr.F.B = "Torsten2"
	err = db.Update("test", &tr)
	if err != nil {
		t.Error(err)
	}

	trPtr = testRowJsonPtr{B: &jt, F: &myStruct{A: "JsonTest", B: "Tom"}}
	err = db.Save("test", &trPtr)
	if err != nil {
		t.Error(err)
	}

	trPtr2 = testRowJsonPtr{B: &jt, F: nil}
	err = db.Save("test", &trPtr2)
	if err != nil {
		t.Error(err)
	}

	err = db.Query(&tr2, "SELECT * FROM test WHERE B = ? ORDER BY A", "JsonTest")
	if err != nil {
		t.Error(err)
	}

	if tr2[0].F.B != tr.F.B {
		t.Errorf(`Error reading back json data, expected "%s", got: "%s"`, tr.F.B, tr2[0].F.B)
	}

	if tr2[1].F.B != trPtr.F.B {
		t.Errorf(`Error reading back json data, expected "%s", got: "%s"`, trPtr.F.B, tr2[1].F.B)
	}

	// pretty.Println(tr2)
	// db.PrintQuery("SELECT *, F IS NULL FROM test")
}

func TestUint8(t *testing.T) {
	var (
		tr, tr2, tr3 testRowUint8
		err          error
	)

	tr = testRowUint8{F: json.RawMessage([]byte("Torsten"))}
	err = db.Insert("test", &tr)
	if err != nil {
		t.Error(err)
	}

	tr2 = testRowUint8{}

	err = db.Insert("test", &tr2)
	if err != nil {
		t.Error(err)
	}

	err = db.Query(&tr3, "SELECT * FROM test WHERE A=?", tr.A)
	if err != nil {
		t.Error(err)
	}

	if string(tr3.F) != string(tr.F) {
		t.Errorf("Expected %s got %s", string(tr.F), string(tr3.F))
	}

	err = db.Query(&tr3, "SELECT * FROM test WHERE A=?", tr2.A)
	if err != nil {
		t.Error(err)
	}

	if tr3.F != nil {
		t.Errorf("Expected <nil> got %s", string(tr3.F))
	}

}

func TestUint8Ptr(t *testing.T) {
	var (
		tr, tr2, tr3 testRowUint8Ptr
		err          error
	)

	rm := json.RawMessage([]byte("Torsten"))

	tr = testRowUint8Ptr{F: &rm}
	err = db.Insert("test", &tr)
	if err != nil {
		t.Error(err)
	}

	tr2 = testRowUint8Ptr{}

	err = db.Insert("test", &tr2)
	if err != nil {
		t.Error(err)
	}

	err = db.Query(&tr3, "SELECT * FROM test WHERE A=?", tr.A)
	if err != nil {
		t.Error(err)
	}

	if string(*tr.F) != string(*tr3.F) {
		t.Errorf("Expected %s got %s", string(*tr.F), string(*tr3.F))
	}

	err = db.Query(&tr3, "SELECT * FROM test WHERE A=?", tr2.A)
	if err != nil {
		t.Error(err)
	}

	if tr3.F != nil {
		t.Errorf("Expected <nil> got %s", string(*tr3.F))
	}

}

type phTest struct {
	sql         string
	args        interface{}
	expSql      string
	expErr      bool
	expArgCount int
}

type ifcArr []interface{}

func TestReplaceArgs(t *testing.T) {

	db2 := New(db.db)

	int_args := []int64{1, 3, 4, 5}
	string_args := []string{"a", "b", "c"}

	db2.PlaceholderMode = QUESTION

	runPlaceholderTests(t, db2, []phTest{
		// sql, args, expected, err?
		{"SELECT * FROM @ WHERE id IN ?", ifcArr{"test", []int64{-1, -2, -3}}, `SELECT * FROM "test" WHERE id IN (?,?,?)`, false, 3},
		{"ID IN ?", ifcArr{int_args}, "ID IN (?,?,?,?)", false, 4},
		{"ID IN '??'", ifcArr{}, "ID IN '??'", false, 0},
		{"ID = ?", ifcArr{"hen'k"}, "ID = ?", false, 1},
		{"ID = ?", ifcArr{5}, "ID = ?", false, 1},
		{"ID IN '''", ifcArr{}, "ID IN '''", false, 0},
		{"ID IN '?'''", ifcArr{}, "ID IN '?'''", false, 0},
		{
			`FROM """value_lat?est" v1 WHERE (((v1."int"::text IN ? OR (v1."text" ILIKE '%berg?see.jpg%' ESCAPE '\')) args)`,
			ifcArr{[]string{`berg?see`}},
			`FROM """value_lat?est" v1 WHERE (((v1."int"::text IN (?) OR (v1."text" ILIKE '%berg?see.jpg%' ESCAPE '\')) args)`,
			false,
			1,
		},
		{"ID IN '?''' WHERE ?", ifcArr{int_args}, "ID IN '?''' WHERE (?,?,?,?)", false, 4},
		{"ID IN ?", ifcArr{string_args}, "ID IN (?,?,?)", false, 3},
	})

	db2.PlaceholderMode = DOLLAR

	runPlaceholderTests(t, db2, []phTest{
		{"ID IN ?", ifcArr{int_args}, "ID IN ($1,$2,$3,$4)", false, 4},
	})

}

func runPlaceholderTests(t *testing.T, db *DB, phTests []phTest) {
	var (
		sqlS    string
		err     error
		newArgs []interface{}
	)

	for idx2, te := range phTests {
		println(fmt.Sprintf("--- #%d ---", idx2))

		args := make([]interface{}, 0)
		switch v := te.args.(type) {
		case []int64:
			for _, arg := range v {
				args = append(args, arg)
			}
		case []string:
			for _, arg := range v {
				args = append(args, arg)
			}
		case ifcArr:
			for _, arg := range v {
				args = append(args, arg)
			}
		default:
			panic(fmt.Sprintf("Unsupported type %T in test.", te.args))
		}
		// pretty.Println(args)
		sqlS, newArgs, err = db.replaceArgs(te.sql, args...)
		if err != nil {
			if te.expErr {
				println(err.Error())
				continue
			}
			t.Error(err)
			return
		} else {
			if te.expErr {
				t.Errorf("Error expected for: %s", te.sql)
				return
			}
			if sqlS != te.expSql {
				t.Errorf("Replace\n[%s] not matching\n[%s]", sqlS, te.expSql)
				return
			}
			if len(newArgs) != te.expArgCount {
				t.Errorf("Expected arg count wrong: %s, exp: %d", sqlS, te.expArgCount)
				return
			}
		}
	}
}

type testEmbedA struct {
	A int64 `db:"a1,pk,omitempty"`
}

type testEmbedB struct {
	testEmbedA
	B string `db:"b"`
}

type testEmbedC struct {
	testEmbedB
	C string `db:"c"`
}

type testEmbed struct {
	testEmbedC
	D string `db:"d"`
}

func TestEmbed(t *testing.T) {
	tr := testEmbed{
		testEmbedC: testEmbedC{
			testEmbedB: testEmbedB{
				testEmbedA: testEmbedA{A: 0},
				B:          "B",
			},
			C: "C",
		},
		D: "D",
	}
	err := db.Save("test", &tr)
	if !assert.NoError(t, err) {
		return
	}
	if !assert.Greater(t, tr.A, int64(0)) {
		return
	}
}
