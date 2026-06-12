package sqlpro

// Gap-closing tests for the remaining reachable branches of the exported API:
// rare error paths, debug-log branches and the less common pointer widths in
// the scan readback. Driven by `go tool cover` against the fake driver.

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"fmt"
	"reflect"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type badJsonRow struct {
	ID int64  `db:"id,pk,omitempty"`
	F  func() `db:"f,json"` // json.Marshal fails on funcs
}

// valuesFromStruct errors must surface through every write entry point
func TestFakeWriteValuesFromStructErrors(t *testing.T) {
	db, _ := newFakeSqlPro(t, SQLITE3)
	bad := badJsonRow{ID: 1, F: func() {}}

	for name, call := range map[string]func() error{
		"Insert":     func() error { return db.Insert("t", &bad) },
		"InsertBulk": func() error { return db.InsertBulk("t", []badJsonRow{bad}) },
		"Update":     func() error { return db.Update("t", &bad) },
		"UpdateBulk": func() error { return db.UpdateBulk("t", []badJsonRow{bad}) },
		"Save":       func() error { return db.Save("t", &bad) },
	} {
		err := call()
		require.Error(t, err, name)
		assert.Contains(t, err.Error(), "marshal", name)
	}

	assert.Error(t, db.InsertBulk("t", 5), "InsertBulk checkData error")
}

func TestFakeUpdateTwoPrimaryKeys(t *testing.T) {
	type twoPk struct {
		A int64  `db:"a,pk"`
		B int64  `db:"b,pk"`
		C string `db:"c"`
	}
	db, backend := newFakeSqlPro(t, SQLITE3)

	backend.queueExec(1, 0)
	require.NoError(t, db.Update("t", &twoPk{A: 1, B: 2, C: "x"}))
	st, _ := backend.lastStatement("exec")
	assert.Contains(t, st.sql, " AND ", "both pks joined in the WHERE clause")

	backend.queueExec(1, 0)
	require.NoError(t, db.UpdateBulk("t", []twoPk{{A: 1, B: 2, C: "x"}}))
	st, _ = backend.lastStatement("exec")
	assert.Contains(t, st.sql, " AND ")
}

func TestFakeInsertPostgresReturningQueryError(t *testing.T) {
	db, backend := newFakeSqlPro(t, POSTGRES)

	backend.queueQueryErr(fmt.Errorf("returning broken"))
	err := db.Insert("t", &fakeRow{Name: "x"})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "returning broken")
}

func TestFakeExecRowsAffectedError(t *testing.T) {
	db, backend := newFakeSqlPro(t, SQLITE3)

	// a RowsAffected error is swallowed; the call reports 0 rows, no insert id
	backend.queueExecResult(fakeExecResult{rowsAffectedErr: fmt.Errorf("no count")})
	rowsAffected, insertID, err := db.ExecContextRowsAffected(context.Background(), "UPDATE t SET a = 1")
	assert.NoError(t, err)
	assert.Equal(t, int64(0), rowsAffected)
	assert.Equal(t, int64(0), insertID)
}

// --- ExecTX rare paths -----------------------------------------------------------

func TestFakeExecTXClosedDB(t *testing.T) {
	db, _ := newFakeSqlPro(t, SQLITE3)
	require.NoError(t, db.Close())

	err := db.ExecTX(context.Background(), func(ctx context.Context) error { return nil }, nil)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "conn:")
}

func TestFakeExecTXBeginError(t *testing.T) {
	db, backend := newFakeSqlPro(t, SQLITE3)

	backend.beginErr = fmt.Errorf("begin broken")
	err := db.ExecTX(context.Background(), func(ctx context.Context) error { return nil }, nil)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "begin:")
}

func TestFakeExecTXSqlitePragmaError(t *testing.T) {
	db, backend := newFakeSqlPro(t, SQLITE3)

	backend.queueExec(0, 0)                       // ROLLBACK; BEGIN IMMEDIATE succeeds
	backend.queueExecErr(fmt.Errorf("no pragma")) // PRAGMA defer_foreign_keys fails
	err := db.ExecTX(context.Background(), func(ctx context.Context) error { return nil }, nil)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "no pragma")
	assert.Equal(t, []string{"ROLLBACK"}, backend.recorded("rollback"))
}

// --- scan readback: remaining pointer widths and direct-pointer fields ------------

func TestFakeScanPointerWidthsSingleStruct(t *testing.T) {
	type widths struct {
		I64  *int64   `db:"i64"`
		I16  *int16   `db:"i16"`
		I8   *int8    `db:"i8"`
		I    *int     `db:"i"`
		U64  *uint64  `db:"u64"`
		U16  *uint16  `db:"u16"`
		U8   *uint8   `db:"u8"`
		U    *uint    `db:"u"`
		F64  *float64 `db:"f64"`
		Blob *[]byte  `db:"blob"` // direct pointer scan (no null scanner)
	}
	db, backend := newFakeSqlPro(t, SQLITE3)

	cols := []string{"i64", "i16", "i8", "i", "u64", "u16", "u8", "u", "f64", "blob"}
	backend.queueQuery(cols, [][]driver.Value{{
		int64(1), int64(2), int64(3), int64(4),
		int64(5), int64(6), int64(7), int64(8),
		9.5, []byte{1},
	}})
	got := widths{}
	require.NoError(t, db.Query(&got, "SELECT * FROM t"))
	require.NotNil(t, got.I64)
	assert.Equal(t, int64(1), *got.I64)
	require.NotNil(t, got.I16)
	assert.Equal(t, int16(2), *got.I16)
	require.NotNil(t, got.I8)
	assert.Equal(t, int8(3), *got.I8)
	require.NotNil(t, got.I)
	assert.Equal(t, 4, *got.I)
	require.NotNil(t, got.U64)
	assert.Equal(t, uint64(5), *got.U64)
	require.NotNil(t, got.U16)
	assert.Equal(t, uint16(6), *got.U16)
	require.NotNil(t, got.U8)
	assert.Equal(t, uint8(7), *got.U8)
	require.NotNil(t, got.U)
	assert.Equal(t, uint(8), *got.U)
	require.NotNil(t, got.F64)
	assert.Equal(t, 9.5, *got.F64)
	require.NotNil(t, got.Blob)
	assert.Equal(t, []byte{1}, *got.Blob)

	// the same struct through the slice fast path (kindDirect pointer branch)
	backend.queueQuery(cols, [][]driver.Value{{
		int64(1), int64(2), int64(3), int64(4),
		int64(5), int64(6), int64(7), int64(8),
		9.5, []byte{1},
	}})
	rows := []widths{}
	require.NoError(t, db.Query(&rows, "SELECT * FROM t"))
	require.Len(t, rows, 1)
	require.NotNil(t, rows[0].U8)
	assert.Equal(t, uint8(7), *rows[0].U8)
	require.NotNil(t, rows[0].Blob)
	assert.Equal(t, []byte{1}, *rows[0].Blob)
}

func TestFakeScanRowError(t *testing.T) {
	db, backend := newFakeSqlPro(t, SQLITE3)

	type tmRow struct {
		TM time.Time `db:"tm"`
	}
	// scanRow (single struct) propagates rows.Scan errors
	backend.queueQuery([]string{"tm"}, fakeValueRows("not-a-time"))
	got := tmRow{}
	assert.Error(t, db.Query(&got, "SELECT tm FROM t"))

	// generic slice mode (scalar rows) propagates them, too
	backend.queueQuery([]string{"tm"}, fakeValueRows("not-a-time"))
	times := []time.Time{}
	assert.Error(t, db.Query(&times, "SELECT tm FROM t"))
}

func TestFakeScanClosedRows(t *testing.T) {
	db, backend := newFakeSqlPro(t, SQLITE3)

	// scanStructSlice reads Columns() before Next(); closed rows error there
	backend.queueQuery([]string{"a"}, fakeValueRows("x"))
	var rows *sql.Rows
	require.NoError(t, db.Query(&rows, "SELECT a FROM t"))
	require.NoError(t, rows.Close())
	target := []fakeRow{}
	assert.Error(t, Scan(&target, rows))
}

// --- debug-log branches ------------------------------------------------------------

func TestFakeDebugLogBranches(t *testing.T) {
	db, backend := newFakeSqlPro(t, SQLITE3)
	db.DebugExec = true

	tx, err := db.Begin() // logs BEGIN
	require.NoError(t, err)
	require.NoError(t, tx.Exec("DELETE FROM t")) // logs SQL
	require.NoError(t, tx.Commit())              // logs COMMIT

	tx, err = db.Begin()
	require.NoError(t, err)
	require.NoError(t, tx.Rollback()) // logs ROLLBACK

	// debugError logs when Debug is set
	db.Debug = true
	backend.queueExecErr(fmt.Errorf("boom"))
	assert.Error(t, db.Exec("DELETE FROM t"))
}

func TestFakeInsertPostgresReturningDebugLog(t *testing.T) {
	db, backend := newFakeSqlPro(t, POSTGRES)
	db.Debug = true // INSERT prefix skips PrintQueryContext, only logs

	backend.queueQuery([]string{"id"}, fakeValueRows(int64(3)))
	in := fakeRow{Name: "x"}
	require.NoError(t, db.Insert("t", &in))
	assert.Equal(t, int64(3), in.ID)
}

func TestFakeQueryDebugPrintErrorPanics(t *testing.T) {
	db, backend := newFakeSqlPro(t, SQLITE3)
	db.Debug = true

	// in debug mode a failing PrintQueryContext panics by design
	backend.queueQueryErr(fmt.Errorf("broken"))
	var s string
	assert.Panics(t, func() {
		_ = db.Query(&s, "SELECT a FROM t")
	})
}

// --- copyFrom guards (without pgx) ---------------------------------------------------

func TestFakeCopyFromPanicsWithoutPgx(t *testing.T) {
	db, _ := newFakeSqlPro(t, SQLITE3)
	assert.Panics(t, func() {
		_ = db.copyFrom(context.Background(), "t", nil)
	})
}

func TestFakeCopyFromDataValuesRecovers(t *testing.T) {
	type panicRow struct {
		X struct{ Y int } `db:"x"` // valueForInsert panics on this
	}
	d := pureDB()
	rows := []panicRow{{X: struct{ Y int }{Y: 1}}}
	info := getStructInfo(reflect.TypeOf(panicRow{}))
	cfd := newCopyFromData(d, map[string]*fieldInfo{"x": info["x"]}, []string{"x"}, reflect.ValueOf(rows))
	require.True(t, cfd.Next())
	_, err := cfd.Values()
	require.Error(t, err)
	assert.Contains(t, err.Error(), "panic")
}

// --- Open: unregistered driver name ---------------------------------------------------

func TestFakeOpenSqlite3NotRegistered(t *testing.T) {
	// only modernc.org/sqlite ("sqlite") is imported by the tests; the mattn
	// driver name "sqlite3" is unregistered and fails inside sql.Open
	_, err := Open("sqlite3", "file:foo.db")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "unknown driver")
}
