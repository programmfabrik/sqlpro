package sqlpro

// Query, placeholder-rewriting and scan paths tested against the fake driver
// (fakedb_test.go). The fake records the SQL that reaches the driver, so the
// DOLLAR rewriting, IN-expansion and literal-inlining of replaceArgs can be
// asserted exactly; canned rows exercise the scanners without a real DB.

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"encoding/json"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// --- replaceArgs / placeholder rewriting --------------------------------------

func TestFakeQueryDollarPlaceholders(t *testing.T) {
	db, backend := newFakeSqlPro(t, POSTGRES)

	backend.queueQuery([]string{"id"}, fakeValueRows(int64(1)))
	var id int64
	require.NoError(t, db.Query(&id, "SELECT id FROM @ WHERE a = ? AND b IN ?", "tbl", 5, []int64{7, 8}))
	assert.Equal(t, int64(1), id)

	st, ok := backend.lastStatement("query")
	require.True(t, ok)
	assert.Equal(t, `SELECT id FROM "tbl" WHERE a = $1 AND b IN ($2,$3)`, st.sql)
	assert.Equal(t, []driver.Value{int64(5), int64(7), int64(8)}, st.args)
}

func TestFakeQueryQuotedStrings(t *testing.T) {
	db, backend := newFakeSqlPro(t, POSTGRES)

	backend.queueQuery([]string{"a"}, fakeValueRows("x"))
	var s string
	// placeholders inside quotes are left alone, escaped quotes ('', "") are
	// skipped correctly
	require.NoError(t, db.Query(&s, `SELECT 'lit ? one', "co""l ?", 'it''s ?' WHERE a = ?`, 1))

	st, _ := backend.lastStatement("query")
	assert.Equal(t, `SELECT 'lit ? one', "co""l ?", 'it''s ?' WHERE a = $1`, st.sql)
}

func TestFakeQueryKeyPlaceholder(t *testing.T) {
	db, backend := newFakeSqlPro(t, SQLITE3)

	name := "col"
	backend.queueQuery([]string{"a"}, fakeValueRows("x"))
	var s string
	require.NoError(t, db.Query(&s, "SELECT @ FROM @", &name, "tbl"))
	st, _ := backend.lastStatement("query")
	assert.Equal(t, `SELECT "col" FROM "tbl"`, st.sql)

	err := db.Query(&s, "SELECT @", 5)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "need *string or string")
}

func TestFakeQueryArgErrors(t *testing.T) {
	db, _ := newFakeSqlPro(t, SQLITE3)

	var s string
	err := db.Query(&s, "SELECT a WHERE a = ? AND b = ?", 1)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "Expecting #2 arg")

	err = db.Query(&s, "SELECT a WHERE a IN ?", []int64{})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "empty slice")
}

func TestFakeQueryInClauseLiterals(t *testing.T) {
	db, backend := newFakeSqlPro(t, SQLITE3)
	db.MaxPlaceholder = 1 // force literal inlining for slices longer than 1

	s1 := "a'b"
	i1, i32a, i64a := 5, int32(6), int64(7)
	backend.queueQuery([]string{"a"}, fakeValueRows("x"))
	var s string
	require.NoError(t, db.Query(&s,
		"SELECT a WHERE s IN ? AND sp IN ? AND i IN ? AND i32 IN ? AND i64 IN ? AND ip IN ? AND i32p IN ? AND i64p IN ?",
		[]string{"a'b", "c"},
		[]*string{&s1, nil},
		[]int{5, 6},
		[]int32{6, 7},
		[]int64{7, 8},
		[]*int{&i1, nil},
		[]*int32{&i32a, nil},
		[]*int64{&i64a, nil},
	))

	st, _ := backend.lastStatement("query")
	assert.Equal(t, "SELECT a WHERE s IN ('a''b','c') AND sp IN ('a''b',null) "+
		"AND i IN (5,6) AND i32 IN (6,7) AND i64 IN (7,8) AND ip IN (5,null) "+
		"AND i32p IN (6,null) AND i64p IN (7,null)", st.sql)

	err := db.Query(&s, "SELECT a WHERE b IN ?", []bool{true, false})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "Unable to add type")
}

func TestFakeQueryTimeArgFormatting(t *testing.T) {
	// SQLITE3 mode formats time args using timeFormat
	db, backend := newFakeSqlPro(t, SQLITE3)

	tm := time.Date(2023, 1, 2, 3, 4, 5, 6, time.UTC)
	backend.queueQuery([]string{"a"}, fakeValueRows("x"))
	var s string
	require.NoError(t, db.Query(&s, "SELECT a WHERE tm = ?", tm))
	st, _ := backend.lastStatement("query")
	assert.Equal(t, []driver.Value{tm.Format(time.RFC3339Nano)}, st.args)

	// convertible named type is detected via toTime
	type simpleTime time.Time
	backend.queueQuery([]string{"a"}, fakeValueRows("x"))
	require.NoError(t, db.Query(&s, "SELECT a WHERE tm = ?", simpleTime(tm)))
	st, _ = backend.lastStatement("query")
	assert.Equal(t, []driver.Value{tm.Format(time.RFC3339Nano)}, st.args)

	// json.RawMessage is passed through as a value
	backend.queueQuery([]string{"a"}, fakeValueRows("x"))
	require.NoError(t, db.Query(&s, "SELECT a WHERE j = ?", json.RawMessage(`{"a":1}`)))
	st, _ = backend.lastStatement("query")
	assert.Equal(t, []driver.Value{[]byte(`{"a":1}`)}, st.args)
}

func TestFakeQueryLeftoverArgs(t *testing.T) {
	db, backend := newFakeSqlPro(t, SQLITE3)

	// surplus args are appended untouched
	backend.queueQuery([]string{"a"}, fakeValueRows("x"))
	var s string
	require.NoError(t, db.Query(&s, "SELECT a WHERE a = ?", 1, 2))
	st, _ := backend.lastStatement("query")
	assert.Len(t, st.args, 2)
}

// --- scanning ------------------------------------------------------------------

type scanAll struct {
	S    string           `db:"s"`
	SP   *string          `db:"sp"`
	I    int              `db:"i"`
	IP   *int32           `db:"ip"`
	U    uint16           `db:"u"`
	UP   *uint32          `db:"up"`
	F    float64          `db:"f"`
	FP   *float32         `db:"fp"`
	B    bool             `db:"b"`
	BP   *bool            `db:"bp"`
	TM   time.Time        `db:"tm"`
	TMP  *time.Time       `db:"tmp"`
	Raw  json.RawMessage  `db:"raw"`
	RawP *json.RawMessage `db:"rawp"`
	JS   map[string]int   `db:"js,json"`
	Blob []byte           `db:"blob"`
}

var scanAllCols = []string{"s", "sp", "i", "ip", "u", "up", "f", "fp", "b", "bp", "tm", "tmp", "raw", "rawp", "js", "blob", "unmapped"}

func scanAllRowValid(tm time.Time) []driver.Value {
	return []driver.Value{
		"hello", "world", int64(1), int64(2), int64(3), int64(4),
		1.5, 2.5, true, false, tm, tm,
		[]byte(`{"r":1}`), []byte(`{"r":2}`), []byte(`{"k":7}`), []byte{1, 2},
		"ignored",
	}
}

func scanAllRowNull() []driver.Value {
	return []driver.Value{
		nil, nil, nil, nil, nil, nil,
		nil, nil, nil, nil, nil, nil,
		nil, nil, nil, nil,
		nil,
	}
}

func assertScanAllValid(t *testing.T, got *scanAll, tm time.Time) {
	t.Helper()
	assert.Equal(t, "hello", got.S)
	require.NotNil(t, got.SP)
	assert.Equal(t, "world", *got.SP)
	assert.Equal(t, 1, got.I)
	require.NotNil(t, got.IP)
	assert.Equal(t, int32(2), *got.IP)
	assert.Equal(t, uint16(3), got.U)
	require.NotNil(t, got.UP)
	assert.Equal(t, uint32(4), *got.UP)
	assert.Equal(t, 1.5, got.F)
	require.NotNil(t, got.FP)
	assert.Equal(t, float32(2.5), *got.FP)
	assert.True(t, got.B)
	require.NotNil(t, got.BP)
	assert.False(t, *got.BP)
	assert.True(t, tm.Equal(got.TM))
	require.NotNil(t, got.TMP)
	assert.Equal(t, json.RawMessage(`{"r":1}`), got.Raw)
	require.NotNil(t, got.RawP)
	assert.Equal(t, json.RawMessage(`{"r":2}`), *got.RawP)
	assert.Equal(t, map[string]int{"k": 7}, got.JS)
	assert.Equal(t, []byte{1, 2}, got.Blob)
}

func assertScanAllNull(t *testing.T, got *scanAll) {
	t.Helper()
	assert.Equal(t, "", got.S)
	assert.Nil(t, got.SP)
	assert.Equal(t, 0, got.I)
	assert.Nil(t, got.IP)
	assert.Equal(t, uint16(0), got.U)
	assert.Nil(t, got.UP)
	assert.Equal(t, 0.0, got.F)
	assert.Nil(t, got.FP)
	assert.False(t, got.B)
	assert.Nil(t, got.BP)
	assert.True(t, got.TM.IsZero())
	assert.Nil(t, got.TMP)
	assert.Nil(t, got.Raw)
	assert.Nil(t, got.RawP)
	assert.Nil(t, got.JS)
	assert.Nil(t, got.Blob)
}

func TestFakeScanStructSlice(t *testing.T) {
	db, backend := newFakeSqlPro(t, SQLITE3)
	tm := time.Date(2023, 1, 2, 3, 4, 5, 0, time.UTC)

	// alternating valid/null rows exercise resetScanner between rows
	backend.queueQuery(scanAllCols, [][]driver.Value{scanAllRowValid(tm), scanAllRowNull(), scanAllRowValid(tm)})
	rows := []scanAll{}
	require.NoError(t, db.Query(&rows, "SELECT * FROM t"))
	require.Len(t, rows, 3)
	assertScanAllValid(t, &rows[0], tm)
	assertScanAllNull(t, &rows[1])
	assertScanAllValid(t, &rows[2], tm)
}

func TestFakeScanStructPtrSlice(t *testing.T) {
	db, backend := newFakeSqlPro(t, SQLITE3)
	tm := time.Date(2023, 1, 2, 3, 4, 5, 0, time.UTC)

	backend.queueQuery(scanAllCols, [][]driver.Value{scanAllRowValid(tm), scanAllRowNull()})
	rows := []*scanAll{}
	require.NoError(t, db.Query(&rows, "SELECT * FROM t"))
	require.Len(t, rows, 2)
	assertScanAllValid(t, rows[0], tm)
	assertScanAllNull(t, rows[1])
}

func TestFakeScanSingleStruct(t *testing.T) {
	db, backend := newFakeSqlPro(t, SQLITE3)
	tm := time.Date(2023, 1, 2, 3, 4, 5, 0, time.UTC)

	// scanRow path (single struct, not the slice fast-path)
	backend.queueQuery(scanAllCols, [][]driver.Value{scanAllRowValid(tm)})
	got := scanAll{}
	require.NoError(t, db.Query(&got, "SELECT * FROM t"))
	assertScanAllValid(t, &got, tm)

	backend.queueQuery(scanAllCols, [][]driver.Value{scanAllRowNull()})
	got = scanAll{}
	require.NoError(t, db.Query(&got, "SELECT * FROM t"))
	assertScanAllNull(t, &got)
}

func TestFakeScanTimeFromString(t *testing.T) {
	db, backend := newFakeSqlPro(t, SQLITE3)

	type tmRow struct {
		TM time.Time `db:"tm"`
	}
	// sqlite stores times as RFC3339Nano strings
	backend.queueQuery([]string{"tm"}, fakeValueRows("2023-01-02T03:04:05.000000006Z"))
	got := []tmRow{}
	require.NoError(t, db.Query(&got, "SELECT tm FROM t"))
	require.Len(t, got, 1)
	assert.Equal(t, 6, got[0].TM.Nanosecond())

	backend.queueQuery([]string{"tm"}, fakeValueRows("not-a-time"))
	err := db.Query(&got, "SELECT tm FROM t")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "NullTime.Scan")
}

func TestFakeScanJsonError(t *testing.T) {
	db, backend := newFakeSqlPro(t, SQLITE3)

	type jsRow struct {
		JS map[string]int `db:"js,json"`
	}
	type jsRowIgnore struct {
		JS map[string]int `db:"js,json,json_ignore_error"`
	}

	// invalid JSON -> error (slice fast-path, readbackCol)
	backend.queueQuery([]string{"js"}, fakeValueRows([]byte(`{invalid`)))
	rows := []jsRow{}
	err := db.Query(&rows, "SELECT js FROM t")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "Error unmarshalling")

	// with json_ignore_error the row is kept, field stays zero
	backend.queueQuery([]string{"js"}, fakeValueRows([]byte(`{invalid`)))
	rowsIgnore := []jsRowIgnore{}
	require.NoError(t, db.Query(&rowsIgnore, "SELECT js FROM t"))
	require.Len(t, rowsIgnore, 1)
	assert.Nil(t, rowsIgnore[0].JS)

	// same two paths through scanRow (single-struct mode)
	backend.queueQuery([]string{"js"}, fakeValueRows([]byte(`{invalid`)))
	one := jsRow{}
	assert.Error(t, db.Query(&one, "SELECT js FROM t"))

	backend.queueQuery([]string{"js"}, fakeValueRows([]byte(`{invalid`)))
	oneIgnore := jsRowIgnore{}
	require.NoError(t, db.Query(&oneIgnore, "SELECT js FROM t"))
	assert.Nil(t, oneIgnore.JS)
}

func TestFakeScanScalarShapes(t *testing.T) {
	db, backend := newFakeSqlPro(t, SQLITE3)

	backend.queueQuery([]string{"a"}, fakeValueRows("x", "y"))
	strs := []string{}
	require.NoError(t, db.Query(&strs, "SELECT a FROM t"))
	assert.Equal(t, []string{"x", "y"}, strs)

	backend.queueQuery([]string{"a", "b"}, [][]driver.Value{{"x", "y"}, {"v", "w"}})
	table := [][]string{}
	require.NoError(t, db.Query(&table, "SELECT a, b FROM t"))
	assert.Equal(t, [][]string{{"x", "y"}, {"v", "w"}}, table)

	// scalar single row: only the first column is mapped, the rest is skipped
	backend.queueQuery([]string{"a", "b"}, [][]driver.Value{{int64(42), "skipped"}})
	var n int64
	require.NoError(t, db.Query(&n, "SELECT a, b FROM t"))
	assert.Equal(t, int64(42), n)
}

func TestFakeScanZeroRows(t *testing.T) {
	db, backend := newFakeSqlPro(t, SQLITE3)

	backend.queueQuery([]string{"a"}, nil)
	var n int64
	err := db.Query(&n, "SELECT a FROM t")
	assert.ErrorIs(t, err, ErrQueryReturnedZeroRows)
	assert.NotEqual(t, err, db.LastError, "debugError must not store ErrQueryReturnedZeroRows")

	// slice targets return an empty slice without error
	backend.queueQuery([]string{"a"}, nil)
	rows := []scanAll{}
	require.NoError(t, db.Query(&rows, "SELECT a FROM t"))
	assert.Len(t, rows, 0)
}

func TestFakeScanPanics(t *testing.T) {
	assert.Panics(t, func() { _ = Scan(nil, nil) }, "nil target")
	v := 5
	assert.Panics(t, func() { _ = Scan(v, nil) }, "non-pointer target")
}

func TestFakeQueryRawRows(t *testing.T) {
	db, backend := newFakeSqlPro(t, SQLITE3)

	backend.queueQuery([]string{"a"}, fakeValueRows("x"))
	var rows *sql.Rows
	require.NoError(t, db.Query(&rows, "SELECT a FROM t"))
	require.NotNil(t, rows)
	defer rows.Close()
	assert.True(t, rows.Next(), "raw *sql.Rows handed out unscanned")
}

func TestFakeQueryDriverError(t *testing.T) {
	db, backend := newFakeSqlPro(t, SQLITE3)

	backend.queueQueryErr(fmt.Errorf("relation missing"))
	var n int64
	err := db.Query(&n, "SELECT a FROM t")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "relation missing")
	assert.Contains(t, err.Error(), "Database Error")
}

// --- Version / Name / debug helpers -------------------------------------------

func TestFakeVersionAndName(t *testing.T) {
	pg, pgBackend := newFakeSqlPro(t, POSTGRES)

	pgBackend.queueQuery([]string{"version"}, fakeValueRows("PostgreSQL 16.1"))
	version, err := pg.Version()
	require.NoError(t, err)
	assert.Equal(t, "PostgreSQL 16.1", version)
	st, _ := pgBackend.lastStatement("query")
	assert.Equal(t, "SELECT version()", st.sql)

	pgBackend.queueQuery([]string{"current_database"}, fakeValueRows("apitest"))
	name, err := pg.Name()
	require.NoError(t, err)
	assert.Equal(t, "apitest", name)

	lite, liteBackend := newFakeSqlPro(t, SQLITE3)

	liteBackend.queueQuery([]string{"sqlite_version()"}, fakeValueRows("3.45.0"))
	version, err = lite.Version()
	require.NoError(t, err)
	assert.Equal(t, "Sqlite 3.45.0", version)

	liteBackend.queueQuery([]string{"file"}, fakeValueRows("/tmp/x.db"))
	name, err = lite.Name()
	require.NoError(t, err)
	assert.Equal(t, "/tmp/x.db", name)

	// unknown driver: no query is sent at all
	other, _ := newFakeSqlPro(t, dbDriver("other"))
	version, err = other.Version()
	require.NoError(t, err)
	assert.Equal(t, "<unsupported driver>", version)
	name, err = other.Name()
	require.NoError(t, err)
	assert.Equal(t, "<unsupported driver>", name)

	// error paths
	pgBackend.queueQueryErr(fmt.Errorf("down"))
	_, err = pg.Version()
	assert.Error(t, err)
	pgBackend.queueQueryErr(fmt.Errorf("down"))
	_, err = pg.Name()
	assert.Error(t, err)
}

func TestFakePrintQueryContext(t *testing.T) {
	db, backend := newFakeSqlPro(t, SQLITE3)

	backend.queueQuery([]string{"a", "b"}, [][]driver.Value{{"1", "x"}, {"2", "y"}})
	require.NoError(t, db.PrintQueryContext(context.Background(), "SELECT a, b FROM t"))

	backend.queueQueryErr(fmt.Errorf("broken"))
	err := db.PrintQueryContext(context.Background(), "SELECT a FROM t")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "broken")
}

func TestFakeQueryDebugMode(t *testing.T) {
	db, backend := newFakeSqlPro(t, SQLITE3)
	db.Debug = true

	// debug mode runs PrintQueryContext first, consuming one queued result
	backend.queueQuery([]string{"a"}, fakeValueRows("x"))
	backend.queueQuery([]string{"a"}, fakeValueRows("x"))
	var s string
	require.NoError(t, db.Query(&s, "SELECT a FROM t"))
	assert.Equal(t, "x", s)
	assert.Len(t, backend.recorded("query"), 2)
}

func TestFakeLog(t *testing.T) {
	db, _ := newFakeSqlPro(t, SQLITE3)

	logged := db.Log()
	assert.False(t, db.Debug, "original untouched")
	assert.NotNil(t, logged)
}

func TestFakeIlikeSql(t *testing.T) {
	assert.Equal(t, `ILIKE '%sch\%ule%' ESCAPE '\'`, IlikeSql(POSTGRES, "sch%ule"))
	assert.Equal(t, `LIKE '%schule%' ESCAPE '\' COLLATE NOCASE`, IlikeSql(SQLITE3, "schule"))
	assert.Panics(t, func() { IlikeSql(dbDriver("other"), "x") })
}
