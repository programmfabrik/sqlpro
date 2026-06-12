package sqlpro

// Exec/Insert/Update/Save paths tested against the fake driver (fakedb_test.go).
// These cover code the sqlite suite cannot reach: POSTGRES mode (RETURNING,
// no LastInsertId), UpdateBulk, scripted driver errors and result behavior.

import (
	"context"
	"database/sql/driver"
	"fmt"
	"reflect"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type fakeRow struct {
	ID   int64  `db:"id,pk,omitempty"`
	Name string `db:"name"`
}

// --- Insert ------------------------------------------------------------------

func TestFakeInsertSqliteLastInsertId(t *testing.T) {
	db, backend := newFakeSqlPro(t, SQLITE3)

	backend.queueExec(1, 42)
	in := fakeRow{Name: "x"}
	require.NoError(t, db.Insert("t", &in))
	assert.Equal(t, int64(42), in.ID, "pk written back from LastInsertId")

	st, ok := backend.lastStatement("exec")
	require.True(t, ok)
	assert.Equal(t, `INSERT INTO "t" ("name") VALUES(?)`, st.sql)
	assert.Equal(t, []any{"x"}, []any{st.args[0]})
}

func TestFakeInsertPostgresReturning(t *testing.T) {
	db, backend := newFakeSqlPro(t, POSTGRES)

	backend.queueQuery([]string{"id"}, fakeValueRows(int64(7)))
	in := fakeRow{Name: "x"}
	require.NoError(t, db.Insert("t", &in))
	assert.Equal(t, int64(7), in.ID, "pk written back from RETURNING")

	st, ok := backend.lastStatement("query")
	require.True(t, ok)
	assert.Equal(t, `INSERT INTO "t" ("name") VALUES($1) RETURNING "id"`, st.sql)
}

func TestFakeInsertPostgresWithoutPk(t *testing.T) {
	type noPk struct {
		Name string `db:"name"`
	}
	db, backend := newFakeSqlPro(t, POSTGRES)

	// no pk -> plain INSERT, SupportsLastInsertId=false skips LastInsertId
	backend.queueExec(1, 0)
	require.NoError(t, db.Insert("t", &noPk{Name: "x"}))
	st, _ := backend.lastStatement("exec")
	assert.Equal(t, `INSERT INTO "t" ("name") VALUES($1)`, st.sql)
}

func TestFakeInsertSliceMode(t *testing.T) {
	db, backend := newFakeSqlPro(t, SQLITE3)

	backend.queueExec(1, 11)
	backend.queueExec(1, 12)
	rows := []*fakeRow{{Name: "a"}, {Name: "b"}}
	require.NoError(t, db.Insert("t", rows))
	assert.Equal(t, int64(11), rows[0].ID)
	assert.Equal(t, int64(12), rows[1].ID)
}

func TestFakeInsertNonInt64PkSingle(t *testing.T) {
	type row32 struct {
		ID   int32  `db:"id,pk,omitempty"`
		Name string `db:"name"`
	}
	db, backend := newFakeSqlPro(t, SQLITE3)

	backend.queueExec(1, 9)
	in := row32{Name: "x"}
	require.NoError(t, db.Insert("t", &in))
	assert.Equal(t, int32(9), in.ID, "non-int64 pk set in struct mode")
}

func TestFakeInsertStringPkNotWrittenBack(t *testing.T) {
	type rowStr struct {
		Code string `db:"code,pk"`
		Name string `db:"name"`
	}
	db, backend := newFakeSqlPro(t, SQLITE3)

	backend.queueExec(1, 5)
	in := rowStr{Code: "k", Name: "x"}
	require.NoError(t, db.Insert("t", &in))
	assert.Equal(t, "k", in.Code, "string pk left alone")
}

func TestFakeInsertMismatchedRowsAffected(t *testing.T) {
	db, backend := newFakeSqlPro(t, SQLITE3)

	backend.queueExec(0, 0)
	err := db.Insert("t", &fakeRow{Name: "x"})
	assert.ErrorIs(t, err, ErrMismatchedRowsAffected)
}

func TestFakeInsertReadOnlyTransaction(t *testing.T) {
	db, _ := newFakeSqlPro(t, POSTGRES)

	tx, err := db.BeginRead()
	require.NoError(t, err)
	defer func() { _ = tx.Rollback() }()

	// RETURNING path checks the read-only flag before querying
	err = tx.Insert("t", &fakeRow{Name: "x"})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "read-only transaction")
}

// --- checkData ---------------------------------------------------------------

func TestFakeInsertChecksData(t *testing.T) {
	db, _ := newFakeSqlPro(t, SQLITE3)

	assert.Error(t, db.Insert("t", 5), "scalar")
	assert.Error(t, db.Insert("t", []int{1}), "slice of non-struct")
	v := 1
	assert.Error(t, db.Insert("t", []*int{&v}), "slice of ptr to non-struct")
	assert.Error(t, db.Insert("t", "x"), "string")
}

// --- InsertBulk --------------------------------------------------------------

func TestFakeInsertBulk(t *testing.T) {
	db, backend := newFakeSqlPro(t, SQLITE3)

	type one struct {
		Name string `db:"name"`
	}
	backend.queueExec(2, 0)
	require.NoError(t, db.InsertBulk("t", []one{{Name: "a"}, {Name: "b"}}))

	st, ok := backend.lastStatement("exec")
	require.True(t, ok)
	assert.Equal(t, "INSERT INTO \"t\" (\"name\") VALUES \n('a')\n,('b')\n", st.sql)
}

func TestFakeInsertBulkErrors(t *testing.T) {
	db, backend := newFakeSqlPro(t, SQLITE3)

	type one struct {
		Name string `db:"name"`
	}
	assert.Error(t, db.InsertBulk("t", one{Name: "a"}), "struct instead of slice")
	assert.NoError(t, db.InsertBulk("t", []one{}), "empty slice is a no-op")

	backend.queueExec(1, 0) // 2 rows inserted, 1 reported
	err := db.InsertBulk("t", []one{{Name: "a"}, {Name: "b"}})
	assert.ErrorIs(t, err, ErrMismatchedRowsAffected)

	backend.queueExecErr(fmt.Errorf("disk full"))
	err = db.InsertBulk("t", []one{{Name: "a"}})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "disk full")
	assert.Contains(t, err.Error(), "Database Error", "wrapped by sqlError")
}

func TestFakeInsertBulkOnConflictDoNothing(t *testing.T) {
	db, backend := newFakeSqlPro(t, SQLITE3)

	type one struct {
		Name string `db:"name"`
	}

	// rows affected is NOT checked in conflict mode
	backend.queueExec(0, 0)
	require.NoError(t, db.InsertBulkOnConflictDoNothingContext(context.Background(), "t", []one{{Name: "a"}}))
	st, _ := backend.lastStatement("exec")
	assert.True(t, strings.HasSuffix(st.sql, " ON CONFLICT DO NOTHING"), st.sql)

	backend.queueExec(0, 0)
	require.NoError(t, db.InsertBulkOnConflictDoNothingContext(context.Background(), "t", []one{{Name: "a"}}, "name", "kind"))
	st, _ = backend.lastStatement("exec")
	assert.True(t, strings.HasSuffix(st.sql, ` ON CONFLICT ("name","kind") DO NOTHING`), st.sql)
}

// --- UpdateBulk --------------------------------------------------------------

func TestFakeUpdateBulk(t *testing.T) {
	db, backend := newFakeSqlPro(t, SQLITE3)

	backend.queueExec(1, 0)
	require.NoError(t, db.UpdateBulk("t", []fakeRow{{ID: 1, Name: "a"}, {ID: 2, Name: "b"}}))

	st, ok := backend.lastStatement("exec")
	require.True(t, ok)
	assert.Equal(t, "UPDATE \"t\" SET \"name\"='a' WHERE \"id\"=1;\nUPDATE \"t\" SET \"name\"='b' WHERE \"id\"=2;\n", st.sql)
}

func TestFakeUpdateBulkErrors(t *testing.T) {
	db, backend := newFakeSqlPro(t, SQLITE3)

	assert.Error(t, db.UpdateBulk("t", fakeRow{ID: 1}), "struct instead of slice")
	assert.Error(t, db.UpdateBulk("t", 5), "checkData error")
	assert.NoError(t, db.UpdateBulk("t", []fakeRow{}), "empty slice is a no-op")

	type ptrPk struct {
		ID   *int64 `db:"id,pk"`
		Name string `db:"name"`
	}
	err := db.UpdateBulk("t", []ptrPk{{Name: "a"}})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "<nil> primary key")

	backend.queueExec(5, 0) // driver reports unexpected rows affected
	err = db.UpdateBulk("t", []fakeRow{{ID: 1, Name: "a"}})
	assert.ErrorIs(t, err, ErrMismatchedRowsAffected)

	backend.queueExecErr(fmt.Errorf("locked"))
	err = db.UpdateBulk("t", []fakeRow{{ID: 1, Name: "a"}})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "locked")
}

// --- Update ------------------------------------------------------------------

func TestFakeUpdate(t *testing.T) {
	db, backend := newFakeSqlPro(t, POSTGRES)

	backend.queueExec(1, 0)
	require.NoError(t, db.Update("t", &fakeRow{ID: 42, Name: "x"}))

	st, ok := backend.lastStatement("exec")
	require.True(t, ok)
	assert.Equal(t, `UPDATE "t" SET "name"=$1 WHERE "id"=$2`, st.sql)
	assert.Equal(t, []any{"x", int64(42)}, []any{st.args[0], st.args[1]})
}

func TestFakeUpdateSliceMode(t *testing.T) {
	db, backend := newFakeSqlPro(t, SQLITE3)

	backend.queueExec(1, 0)
	backend.queueExec(1, 0)
	require.NoError(t, db.Update("t", []fakeRow{{ID: 1, Name: "a"}, {ID: 2, Name: "b"}}))
	assert.Len(t, backend.recorded("exec"), 2)
}

func TestFakeUpdateErrors(t *testing.T) {
	dbc, backend := newFakeSqlPro(t, SQLITE3)

	type noPk struct {
		Name string `db:"name"`
	}
	err := dbc.Update("t", &noPk{Name: "x"})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "at least one key needed")

	// same error from the slice loop
	err = dbc.Update("t", []noPk{{Name: "x"}})
	assert.Error(t, err)

	type ptrPk struct {
		ID   *int64 `db:"id,pk"`
		Name string `db:"name"`
	}
	err = dbc.Update("t", &ptrPk{Name: "x"})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "<nil> key")

	backend.queueExec(0, 0)
	err = dbc.Update("t", &fakeRow{ID: 1, Name: "a"})
	assert.ErrorIs(t, err, ErrMismatchedRowsAffected)

	backend.queueExec(0, 0)
	err = dbc.Update("t", []fakeRow{{ID: 1, Name: "a"}})
	assert.ErrorIs(t, err, ErrMismatchedRowsAffected, "mismatch in slice mode")

	assert.Error(t, dbc.Update("t", 5), "checkData error")

	assert.Panics(t, func() {
		var nilDB *db
		_ = nilDB.UpdateContext(context.Background(), "t", &fakeRow{})
	}, "Update on nil handle panics")
}

// --- Save --------------------------------------------------------------------

func TestFakeSave(t *testing.T) {
	db, backend := newFakeSqlPro(t, SQLITE3)

	// pk zero -> INSERT
	backend.queueExec(1, 21)
	in := fakeRow{Name: "new"}
	require.NoError(t, db.Save("t", &in))
	st, _ := backend.lastStatement("exec")
	assert.True(t, strings.HasPrefix(st.sql, "INSERT INTO"), st.sql)
	assert.Equal(t, int64(21), in.ID)

	// pk set -> UPDATE
	backend.queueExec(1, 0)
	require.NoError(t, db.Save("t", &in))
	st, _ = backend.lastStatement("exec")
	assert.True(t, strings.HasPrefix(st.sql, "UPDATE"), st.sql)
}

func TestFakeSaveSliceMode(t *testing.T) {
	db, backend := newFakeSqlPro(t, SQLITE3)

	backend.queueExec(1, 31) // INSERT for pk==0
	backend.queueExec(1, 0)  // UPDATE for pk!=0
	rows := []*fakeRow{{Name: "new"}, {ID: 5, Name: "old"}}
	require.NoError(t, db.Save("t", rows))

	execs := backend.recorded("exec")
	require.Len(t, execs, 2)
	assert.True(t, strings.HasPrefix(execs[0], "INSERT"), execs[0])
	assert.True(t, strings.HasPrefix(execs[1], "UPDATE"), execs[1])
}

func TestFakeSaveErrors(t *testing.T) {
	db, _ := newFakeSqlPro(t, SQLITE3)

	type noPk struct {
		Name string `db:"name"`
	}
	err := db.Save("t", &noPk{Name: "x"})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "exactly one 'pk' field")

	type twoPk struct {
		A int64 `db:"a,pk"`
		B int64 `db:"b,pk"`
	}
	assert.Error(t, db.Save("t", &twoPk{}), "two pks -> no single pk")
	assert.Error(t, db.Save("t", 5), "checkData error")

	err = db.Save("t", []noPk{{Name: "x"}})
	assert.Error(t, err, "error inside slice loop")
}

// --- setPrimaryKey -----------------------------------------------------------

func TestFakeSetPrimaryKey(t *testing.T) {
	type pks struct {
		I   int
		I8  int8
		I16 int16
		I32 int32
		I64 int64
		U   uint
		U8  uint8
		U16 uint16
		U32 uint32
		U64 uint64
		S   string
	}
	v := pks{}
	rv := reflect.ValueOf(&v).Elem()
	for i := 0; i < 10; i++ {
		setPrimaryKey(rv.Field(i), 7)
	}
	assert.Equal(t, pks{I: 7, I8: 7, I16: 7, I32: 7, I64: 7, U: 7, U8: 7, U16: 7, U32: 7, U64: 7}, v)

	assert.Panics(t, func() {
		setPrimaryKey(rv.FieldByName("S"), 7)
	}, "string pk panics")
}

// --- execContext details -----------------------------------------------------

func TestFakeExecEmptySQL(t *testing.T) {
	db, _ := newFakeSqlPro(t, SQLITE3)

	assert.Error(t, db.Exec(""), "Exec rejects empty SQL")
	_, _, err := db.ExecContextRowsAffected(context.Background(), "")
	assert.Error(t, err, "ExecContextRowsAffected rejects empty SQL")
}

func TestFakeExecReadOnlyTransaction(t *testing.T) {
	db, _ := newFakeSqlPro(t, SQLITE3)

	tx, err := db.BeginRead()
	require.NoError(t, err)
	defer func() { _ = tx.Rollback() }()

	err = tx.Exec("DELETE FROM t")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "read-only transaction")
}

func TestFakeExecReplaceArgsError(t *testing.T) {
	db, _ := newFakeSqlPro(t, SQLITE3)

	err := db.Exec("DELETE FROM t WHERE a = ? AND b = ?", 1)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "Expecting #2 arg")
}

func TestFakeExecDriverError(t *testing.T) {
	db, backend := newFakeSqlPro(t, SQLITE3)

	backend.queueExecErr(fmt.Errorf("syntax error"))
	err := db.Exec("DELETE FROM t WHERE a = ?", 1)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "syntax error")
	assert.Contains(t, err.Error(), "Database Error", "wrapped by sqlError")
	assert.Equal(t, err, db.LastError, "debugError stores LastError")
}

func TestFakeExecLastInsertIdError(t *testing.T) {
	db, backend := newFakeSqlPro(t, SQLITE3)

	backend.queueExecResult(fakeExecResult{rowsAffected: 1, lastInsertIDErr: fmt.Errorf("no id")})
	_, _, err := db.ExecContextRowsAffected(context.Background(), "INSERT INTO t VALUES (1)")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "no id")
}

func TestFakeExecRowsAffectedPanic(t *testing.T) {
	db, backend := newFakeSqlPro(t, SQLITE3)

	// sqlite panics inside RowsAffected on empty statements; execContext
	// recovers and reports 0 rows without error
	backend.queueExecResult(fakeExecResult{rowsAffectedPanic: true})
	rowsAffected, insertID, err := db.ExecContextRowsAffected(context.Background(), "SELECT 1")
	assert.NoError(t, err)
	assert.Equal(t, int64(0), rowsAffected)
	assert.Equal(t, int64(0), insertID)
}

func TestFakeExecContextRowsAffected(t *testing.T) {
	db, backend := newFakeSqlPro(t, SQLITE3)

	backend.queueExec(3, 17)
	rowsAffected, insertID, err := db.ExecContextRowsAffected(context.Background(), "UPDATE t SET a = 1")
	require.NoError(t, err)
	assert.Equal(t, int64(3), rowsAffected)
	assert.Equal(t, int64(17), insertID)
}

// fakeValueRows builds single-column rows from the given values.
func fakeValueRows(values ...driver.Value) [][]driver.Value {
	rows := make([][]driver.Value, 0, len(values))
	for _, v := range values {
		rows = append(rows, []driver.Value{v})
	}
	return rows
}
