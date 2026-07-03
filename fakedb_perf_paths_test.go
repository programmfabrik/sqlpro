package sqlpro

// Behavior tests for the code paths introduced by the performance work:
// the scalar-slice scan fast path (scanScalarSlice), the streaming bulk
// insert (fieldValueForBulk / bindBulkPlan / bulkRow) and isZeroValue.
// They pin the semantics the fast paths must share with the generic path.

import (
	"database/sql/driver"
	"reflect"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// --- scanScalarSlice ----------------------------------------------------------

func TestFakeScanScalarKinds(t *testing.T) {
	db, backend := newFakeSqlPro(t, SQLITE3)
	tm := time.Date(2023, 1, 2, 3, 4, 5, 0, time.UTC)

	backend.queueQuery([]string{"a"}, fakeValueRows(1.5, 2.5))
	floats := []float64{}
	require.NoError(t, db.Query(&floats, "SELECT a FROM t"))
	assert.Equal(t, []float64{1.5, 2.5}, floats)

	backend.queueQuery([]string{"a"}, fakeValueRows(true, false))
	bools := []bool{}
	require.NoError(t, db.Query(&bools, "SELECT a FROM t"))
	assert.Equal(t, []bool{true, false}, bools)

	backend.queueQuery([]string{"a"}, fakeValueRows(tm, nil))
	times := []time.Time{}
	require.NoError(t, db.Query(&times, "SELECT a FROM t"))
	require.Len(t, times, 2)
	assert.True(t, tm.Equal(times[0]))
	assert.True(t, times[1].IsZero(), "NULL -> zero time, as in the generic path")

	backend.queueQuery([]string{"a"}, fakeValueRows(int64(7), nil))
	uints := []uint32{}
	require.NoError(t, db.Query(&uints, "SELECT a FROM t"))
	assert.Equal(t, []uint32{7, 0}, uints)
}

func TestFakeScanScalarPtrSliceNulls(t *testing.T) {
	db, backend := newFakeSqlPro(t, SQLITE3)

	backend.queueQuery([]string{"a"}, fakeValueRows(int64(5), nil, int64(6)))
	ids := []*int64{}
	require.NoError(t, db.Query(&ids, "SELECT a FROM t"))
	require.Len(t, ids, 3)
	require.NotNil(t, ids[0])
	assert.Equal(t, int64(5), *ids[0])
	assert.Nil(t, ids[1], "NULL -> nil pointer")
	require.NotNil(t, ids[2])
	assert.Equal(t, int64(6), *ids[2])

	backend.queueQuery([]string{"a"}, fakeValueRows("x", nil))
	strs := []*string{}
	require.NoError(t, db.Query(&strs, "SELECT a FROM t"))
	require.Len(t, strs, 2)
	require.NotNil(t, strs[0])
	assert.Equal(t, "x", *strs[0])
	assert.Nil(t, strs[1])
}

func TestFakeScanScalarExtraColumnsIgnored(t *testing.T) {
	db, backend := newFakeSqlPro(t, SQLITE3)

	// only the first column maps, the rest is discarded — as in scanRow
	backend.queueQuery([]string{"a", "b", "c"}, [][]driver.Value{{int64(1), "x", true}, {int64(2), "y", false}})
	ids := []int64{}
	require.NoError(t, db.Query(&ids, "SELECT a, b, c FROM t"))
	assert.Equal(t, []int64{1, 2}, ids)
}

func TestFakeScanScalarNamedTypeKeepsGenericPath(t *testing.T) {
	type myID int64
	kind, ok := scalarScanKind(reflect.TypeOf(myID(0)), false)
	assert.False(t, ok, "named types stay on the generic path")
	_ = kind

	// and the generic path still handles them (direct scan, no null-scanner)
	db, backend := newFakeSqlPro(t, SQLITE3)
	backend.queueQuery([]string{"a"}, fakeValueRows(int64(9)))
	ids := []myID{}
	require.NoError(t, db.Query(&ids, "SELECT a FROM t"))
	assert.Equal(t, []myID{9}, ids)
}

func TestFakeScanScalarPtrTimeExcluded(t *testing.T) {
	_, ok := scalarScanKind(reflect.TypeOf(time.Time{}), true)
	assert.False(t, ok, "[]*time.Time keeps the generic path (NULL semantics differ)")

	_, ok = scalarScanKind(reflect.TypeOf(time.Time{}), false)
	assert.True(t, ok)

	_, ok = scalarScanKind(reflect.TypeOf([]byte(nil)), false)
	assert.False(t, ok, "non-scalar kinds are not fast-pathed")
}

func TestFakeScanScalarRowError(t *testing.T) {
	db, backend := newFakeSqlPro(t, SQLITE3)

	// NullTime parse failure propagates out of the fast path
	backend.queueQuery([]string{"a"}, fakeValueRows("not-a-time"))
	times := []time.Time{}
	assert.Error(t, db.Query(&times, "SELECT a FROM t"))
}

// --- streaming bulk insert ------------------------------------------------------

func TestFakeInsertBulkOmitemptyUnion(t *testing.T) {
	type row struct {
		ID   int64  `db:"id,pk,omitempty"`
		Name string `db:"name"`
		Note string `db:"note,omitempty"`
	}
	db, backend := newFakeSqlPro(t, SQLITE3)

	// the rows qualify for the pk read-back, so the bulk insert runs as a
	// RETURNING query, not an exec

	// note is zero in row 1 but set in row 2 -> column included, row 1 = NULL
	backend.queueQuery([]string{"id"}, fakeValueRows(int64(1), int64(2)))
	require.NoError(t, db.InsertBulk("t", []row{{Name: "a"}, {Name: "b", Note: "n"}}))
	st, _ := backend.lastStatement("query")
	assert.Contains(t, st.sql, `"note"`)
	assert.Contains(t, st.sql, "NULL", "omitted cell of row 1 renders as NULL")
	assert.Contains(t, st.sql, `RETURNING "id"`)

	// note zero in ALL rows -> column not emitted at all
	backend.queueQuery([]string{"id"}, fakeValueRows(int64(3)))
	require.NoError(t, db.InsertBulk("t", []row{{Name: "a"}}))
	st, _ = backend.lastStatement("query")
	assert.NotContains(t, st.sql, `"note"`)
}

func TestFakeInsertBulkJsonAndReadonly(t *testing.T) {
	type row struct {
		Name string         `db:"name"`
		JS   map[string]int `db:"js,json"`
		RO   string         `db:"ro,readonly"`
	}
	db, backend := newFakeSqlPro(t, SQLITE3)

	backend.queueExec(2, 0)
	require.NoError(t, db.InsertBulk("t", []row{
		{Name: "a", JS: map[string]int{"k": 1}, RO: "never"},
		{Name: "b"}, // zero json -> NULL
	}))
	st, _ := backend.lastStatement("exec")
	assert.Contains(t, st.sql, `'{"k":1}'`)
	assert.Contains(t, st.sql, "NULL", "zero json renders as NULL by default")
	assert.NotContains(t, st.sql, `"ro"`, "readonly column never emitted")

	// json marshal error propagates from pass 2
	type badRow struct {
		F func() `db:"f,json"`
	}
	err := db.InsertBulk("t", []badRow{{F: func() {}}})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "marshal")
}

func TestFakeInsertBulkHeterogeneousSlice(t *testing.T) {
	type rowA struct {
		Name string `db:"name"`
	}
	type rowB struct {
		Name string `db:"name"`
		Kind string `db:"kind"`
	}
	db, backend := newFakeSqlPro(t, SQLITE3)

	// []any with different struct types: union of columns, missing cells NULL
	backend.queueExec(2, 0)
	require.NoError(t, db.InsertBulk("t", []any{rowA{Name: "a"}, rowB{Name: "b", Kind: "k"}}))
	st, _ := backend.lastStatement("exec")
	assert.Contains(t, st.sql, `"kind"`)
	assert.Contains(t, st.sql, "NULL", "rowA has no kind -> NULL")
	assert.Contains(t, st.sql, "'k'")
}

func TestFakeInsertBulkHeterogeneousDifferingTags(t *testing.T) {
	// the same column tagged differently on two types: each row is rendered
	// with its own type's rules (rowB's zero "c" -> NULL; rowA's notnull
	// pointer never applies to rowB rows)
	v := "x"
	type rowA struct {
		Name string  `db:"name"`
		C    *string `db:"c,omitempty,notnull"`
	}
	type rowB struct {
		Name string  `db:"name"`
		C    *string `db:"c,omitempty,null"`
	}
	db, backend := newFakeSqlPro(t, SQLITE3)

	backend.queueExec(3, 0)
	require.NoError(t, db.InsertBulk("t", []any{
		rowA{Name: "a", C: &v},
		rowB{Name: "b", C: &v},
		rowB{Name: "c"}, // zero on rowB's null-tagged field -> NULL, no panic
	}))
	st, _ := backend.lastStatement("exec")
	assert.Contains(t, st.sql, "NULL")
}

func TestFakeScanScalarPartialOnError(t *testing.T) {
	db, backend := newFakeSqlPro(t, SQLITE3)

	// rows scanned before a failure stay visible, as in the generic path
	backend.queueQuery([]string{"a"}, fakeValueRows("2023-01-02T03:04:05Z", "not-a-time"))
	times := []time.Time{}
	err := db.Query(&times, "SELECT a FROM t")
	require.Error(t, err)
	assert.Len(t, times, 1, "first row kept on error in row 2")
}

// --- isZeroValue ----------------------------------------------------------------

func TestIsZeroValue(t *testing.T) {
	type s struct {
		I any
	}

	v := reflect.ValueOf(s{}).Field(0)
	assert.True(t, isZeroValue(v), "nil interface field")

	var p *int
	v = reflect.ValueOf(s{I: p}).Field(0)
	assert.True(t, isZeroValue(v), "interface holding typed nil pointer (DeepEqual semantics)")

	x := 5
	v = reflect.ValueOf(s{I: &x}).Field(0)
	assert.False(t, isZeroValue(v))

	v = reflect.ValueOf(s{I: 0}).Field(0)
	assert.True(t, isZeroValue(v), "interface holding zero int")

	assert.True(t, isZeroValue(reflect.ValueOf(nil)), "invalid value counts as zero")

	// equivalence with the documented isZero(any) semantics
	assert.True(t, isZero(time.Time{}))
	assert.False(t, isZero([]int{}), "non-nil empty slice is NOT zero (DeepEqual semantics)")
}
