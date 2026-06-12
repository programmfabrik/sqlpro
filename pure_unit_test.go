package sqlpro

// Pure-logic unit tests: value conversion, struct-tag inspection, the custom
// null scanners and small helpers. No database — not even the fake driver —
// is involved.

import (
	"database/sql/driver"
	"encoding/json"
	"reflect"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// pureDB returns a wrapper without any connection, sufficient for the pure
// conversion helpers.
func pureDB() *db {
	return newSqlPro(nil)
}

// --- valueForInsert ------------------------------------------------------------

type valuerOK struct{ v string }

func (v valuerOK) Value() (driver.Value, error) { return v.v, nil }

type ptrValuer struct{ v int64 }

func (v *ptrValuer) Value() (driver.Value, error) { return v.v, nil }

func TestValueForInsertNumeric(t *testing.T) {
	d := pureDB()
	fi := &fieldInfo{}

	i8, i16, i32, i64, i := int8(1), int16(2), int32(3), int64(4), 5
	u8, u16, u32, u64, u := uint8(1), uint16(2), uint32(3), uint64(4), uint(5)
	f32, f64 := float32(1.5), 2.5
	bTrue, bFalse := true, false

	assert.Equal(t, int64(1), d.valueForInsert(i8, fi))
	assert.Equal(t, int64(1), d.valueForInsert(&i8, fi))
	assert.Equal(t, int64(2), d.valueForInsert(i16, fi))
	assert.Equal(t, int64(2), d.valueForInsert(&i16, fi))
	assert.Equal(t, int64(3), d.valueForInsert(i32, fi))
	assert.Equal(t, int64(3), d.valueForInsert(&i32, fi))
	assert.Equal(t, int64(4), d.valueForInsert(i64, fi))
	assert.Equal(t, int64(4), d.valueForInsert(&i64, fi))
	assert.Equal(t, int64(5), d.valueForInsert(i, fi))
	assert.Equal(t, int64(5), d.valueForInsert(&i, fi))

	assert.Equal(t, int64(1), d.valueForInsert(u8, fi))
	assert.Equal(t, int64(1), d.valueForInsert(&u8, fi))
	assert.Equal(t, int64(2), d.valueForInsert(u16, fi))
	assert.Equal(t, int64(2), d.valueForInsert(&u16, fi))
	assert.Equal(t, int64(3), d.valueForInsert(u32, fi))
	assert.Equal(t, int64(3), d.valueForInsert(&u32, fi))
	assert.Equal(t, int64(4), d.valueForInsert(u64, fi))
	assert.Equal(t, int64(4), d.valueForInsert(&u64, fi))
	assert.Equal(t, int64(5), d.valueForInsert(u, fi))
	assert.Equal(t, int64(5), d.valueForInsert(&u, fi))

	assert.Equal(t, float32(1.5), d.valueForInsert(f32, fi))
	assert.Equal(t, float32(1.5), d.valueForInsert(&f32, fi))
	assert.Equal(t, 2.5, d.valueForInsert(f64, fi))
	assert.Equal(t, 2.5, d.valueForInsert(&f64, fi))

	assert.Equal(t, true, d.valueForInsert(bTrue, fi))
	assert.Equal(t, true, d.valueForInsert(&bTrue, fi))
	assert.Equal(t, false, d.valueForInsert(bFalse, fi))
	assert.Equal(t, false, d.valueForInsert(&bFalse, fi))
}

func TestValueForInsertStringsAndBytes(t *testing.T) {
	d := pureDB()
	fi := &fieldInfo{}

	s := "x"
	assert.Equal(t, "x", d.valueForInsert(s, fi))
	assert.Equal(t, "x", d.valueForInsert(&s, fi))
	assert.Equal(t, "ab", d.valueForInsert([]uint8("ab"), fi))
	assert.Equal(t, `{"a":1}`, d.valueForInsert(json.RawMessage(`{"a":1}`), fi))
}

func TestValueForInsertTime(t *testing.T) {
	d := pureDB()
	fi := &fieldInfo{}
	tm := time.Date(2023, 1, 2, 3, 4, 5, 0, time.UTC)

	// no timeFormat: time passed through
	assert.Equal(t, tm, d.valueForInsert(tm, fi))
	assert.Equal(t, tm, d.valueForInsert(&tm, fi))

	// with timeFormat (sqlite mode): formatted string
	d.timeFormat = time.RFC3339Nano
	assert.Equal(t, "2023-01-02T03:04:05Z", d.valueForInsert(tm, fi))
	assert.Equal(t, "2023-01-02T03:04:05Z", d.valueForInsert(&tm, fi))

	// convertible named time type takes the toTime fallback
	type simpleTime time.Time
	assert.Equal(t, "2023-01-02T03:04:05Z", d.valueForInsert(simpleTime(tm), fi))
	d.timeFormat = ""
	assert.Equal(t, tm, d.valueForInsert(simpleTime(tm), fi))
}

func TestValueForInsertValuer(t *testing.T) {
	d := pureDB()
	fi := &fieldInfo{}

	// value receiver
	assert.Equal(t, "wrapped", d.valueForInsert(valuerOK{v: "wrapped"}, fi))
	// pointer receiver, passed as value: detected via the pointer probe
	assert.Equal(t, int64(9), d.valueForInsert(ptrValuer{v: 9}, fi))
}

func TestValueForInsertNamedKinds(t *testing.T) {
	d := pureDB()
	fi := &fieldInfo{}

	type myInt int
	type myString string
	assert.Equal(t, int64(7), d.valueForInsert(myInt(7), fi))
	assert.Equal(t, "named", d.valueForInsert(myString("named"), fi))

	assert.Panics(t, func() {
		d.valueForInsert(struct{ X int }{X: 1}, fi)
	}, "unsupported type panics")
}

func TestValueForInsertNull(t *testing.T) {
	d := pureDB()

	var p *string
	assert.Nil(t, d.valueForInsert(p, &fieldInfo{ptr: true}), "nil ptr -> NULL")

	assert.Panics(t, func() {
		d.valueForInsert(p, &fieldInfo{ptr: true, notNull: true})
	}, "nil ptr into notnull field panics")
}

// --- escValueForInsert -----------------------------------------------------------

func TestEscValueForInsert(t *testing.T) {
	d := pureDB()
	fi := &fieldInfo{}
	tm := time.Date(2023, 1, 2, 3, 4, 5, 0, time.UTC)

	assert.Equal(t, `'it''s'`, d.escValueForInsert("it's", fi))
	assert.Equal(t, "1.5", d.escValueForInsert(float32(1.5), fi))
	assert.Equal(t, "2.5", d.escValueForInsert(2.5, fi))
	assert.Equal(t, "42", d.escValueForInsert(42, fi))
	assert.Equal(t, "TRUE", d.escValueForInsert(true, fi))
	assert.Equal(t, "FALSE", d.escValueForInsert(false, fi))
	assert.Equal(t, "NULL", d.escValueForInsert(nil, fi))
	// time.Time stays a time.Time without timeFormat and is rendered here
	assert.Equal(t, "'2023-01-02T03:04:05Z'", d.escValueForInsert(tm, fi))
}

// --- toTime / isZero -------------------------------------------------------------

func TestToTime(t *testing.T) {
	tm := time.Date(2023, 1, 2, 3, 4, 5, 0, time.UTC)

	got, ok := toTime(tm)
	assert.True(t, ok)
	assert.Equal(t, tm, got)

	got, ok = toTime(&tm)
	assert.True(t, ok)
	assert.Equal(t, tm, got)

	var nilT *time.Time
	_, ok = toTime(nilT)
	assert.False(t, ok)

	type simpleTime time.Time
	got, ok = toTime(simpleTime(tm))
	assert.True(t, ok)
	assert.Equal(t, tm, got)

	_, ok = toTime(5)
	assert.False(t, ok)

	_, ok = toTime(nil)
	assert.False(t, ok)
}

func TestIsZero(t *testing.T) {
	assert.True(t, isZero(nil))
	assert.True(t, isZero(""))
	assert.True(t, isZero(0))
	assert.True(t, isZero(time.Time{}))
	assert.False(t, isZero("x"))
	assert.False(t, isZero(1))
}

// --- argsToString -----------------------------------------------------------------

func TestArgsToString(t *testing.T) {
	assert.Equal(t, " <none>", argsToString())

	i := 5
	f := 1.5
	s := "x"
	b := true
	out := argsToString(nil, b, i, &i, int64(7), f, &f, s, &s, []byte("z"))
	assert.Contains(t, out, "#1 <nil>")
	assert.Contains(t, out, "bool true")
	assert.Contains(t, out, "int 5")
	assert.Contains(t, out, "*int 5")
	assert.Contains(t, out, "int64 7")
	assert.Contains(t, out, "float64")
	assert.Contains(t, out, "string x")
	assert.Contains(t, out, "*string x")
	assert.Contains(t, out, "#10") // default %v branch
}

// --- struct info ------------------------------------------------------------------

func TestGetStructInfoPanics(t *testing.T) {
	type inner struct {
		A string `db:"a"`
	}
	type embedsPtr struct {
		*inner
	}
	assert.Panics(t, func() {
		getStructInfo(typeOf(embedsPtr{}))
	}, "embedded pointer struct")

	type unexported struct {
		a string `db:"a"` //lint:ignore U1000 the tag on an unexported field is the point
	}
	assert.Panics(t, func() {
		getStructInfo(typeOf(unexported{}))
	}, "db tag on unexported field")
}

func TestGetStructInfoTagOptions(t *testing.T) {
	type tags struct {
		Named   string  `db:"the_name"`
		Auto    string  `db:","`
		Skipped string  `db:"-"`
		NoTag   string  //nolint
		Unknown string  `db:"u,bogus_option"`
		Ptr     *string `db:"p"`
		PtrNN   *string `db:"pnn,notnull"`
		NullS   string  `db:"ns,null"`
	}
	info := getStructInfo(typeOf(tags{}))

	require.Contains(t, info, "the_name")
	require.Contains(t, info, "Auto", "empty tag name falls back to the field name")
	assert.NotContains(t, info, "Skipped")
	assert.NotContains(t, info, "NoTag")
	require.Contains(t, info, "u", "unknown options are ignored")

	assert.True(t, info["p"].allowNull(), "plain pointer accepts null")
	assert.False(t, info["pnn"].allowNull(), "notnull pointer refuses null")
	assert.True(t, info["ns"].allowNull(), "null-tagged value field accepts null")
	assert.False(t, info["the_name"].allowNull())
}

func TestStructInfoPrimaryKey(t *testing.T) {
	type twoPk struct {
		A int64 `db:"a,pk"`
		B int64 `db:"b,pk"`
		C int64 `db:"c"`
	}
	info := getStructInfo(typeOf(twoPk{}))
	assert.True(t, info.primaryKey("a"))
	assert.False(t, info.primaryKey("c"))
	assert.Nil(t, info.onlyPrimaryKey(), "two pks -> nil")
	assert.Panics(t, func() { info.primaryKey("missing") })
}

// --- valuesFromStruct -------------------------------------------------------------

func TestValuesFromStructJson(t *testing.T) {
	d := pureDB()

	type withJson struct {
		JS map[string]int `db:"js,json"`
	}
	values, _, err := d.valuesFromStruct(withJson{JS: map[string]int{"a": 1}})
	require.NoError(t, err)
	assert.Equal(t, []byte(`{"a":1}`), values["js"])

	// zero json value renders to "null" -> stored as NULL by default
	values, _, err = d.valuesFromStruct(withJson{})
	require.NoError(t, err)
	assert.Nil(t, values["js"])

	// notnull keeps the literal "null" instead
	type withJsonNN struct {
		JS map[string]int `db:"js,json,notnull"`
	}
	values, _, err = d.valuesFromStruct(withJsonNN{})
	require.NoError(t, err)
	assert.Equal(t, []byte(`null`), values["js"])

	// unmarshalable type errors out ...
	type withBadJson struct {
		F func() `db:"f,json"`
	}
	_, _, err = d.valuesFromStruct(withBadJson{F: func() {}})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "Unable to marshal")

	// ... unless json_ignore_error is set
	type withBadJsonIgnore struct {
		F func() `db:"f,json,json_ignore_error"`
	}
	_, _, err = d.valuesFromStruct(withBadJsonIgnore{F: func() {}})
	assert.NoError(t, err)
}

// --- Null* scanners ---------------------------------------------------------------

func TestNullTimeScan(t *testing.T) {
	nt := NullTime{}
	require.NoError(t, nt.Scan(nil))
	assert.False(t, nt.Valid)

	tm := time.Date(2023, 1, 2, 3, 4, 5, 0, time.UTC)
	require.NoError(t, nt.Scan(tm))
	assert.True(t, nt.Valid)
	assert.Equal(t, tm, nt.Time)

	require.NoError(t, nt.Scan("2023-01-02T03:04:05Z"))
	assert.True(t, nt.Valid)

	assert.Error(t, nt.Scan("not-a-time"))
	assert.Error(t, nt.Scan(5))
}

func TestNullJsonScan(t *testing.T) {
	nj := NullJson{}
	require.NoError(t, nj.Scan(nil))
	assert.False(t, nj.Valid)
	require.NoError(t, nj.Scan([]byte{}))
	assert.False(t, nj.Valid, "empty bytes stay invalid")
	require.NoError(t, nj.Scan(""))
	assert.False(t, nj.Valid, "empty string stays invalid")

	require.NoError(t, nj.Scan([]byte(`{"a":1}`)))
	assert.True(t, nj.Valid)
	require.NoError(t, nj.Scan(`{"b":2}`))
	assert.Equal(t, []byte(`{"b":2}`), nj.Data)

	assert.Error(t, nj.Scan(5))
}

func TestNullRawMessageScan(t *testing.T) {
	nr := NullRawMessage{}
	require.NoError(t, nr.Scan(nil))
	assert.False(t, nr.Valid)
	require.NoError(t, nr.Scan([]byte{}))
	assert.False(t, nr.Valid)
	require.NoError(t, nr.Scan(""))
	assert.False(t, nr.Valid)

	require.NoError(t, nr.Scan([]byte(`{"a":1}`)))
	assert.True(t, nr.Valid)
	require.NoError(t, nr.Scan(`{"b":2}`))
	assert.Equal(t, json.RawMessage(`{"b":2}`), nr.Data)

	assert.Error(t, nr.Scan(5))
}

// --- Open / Close / IsClosed -------------------------------------------------------

func TestOpenUnknownDriver(t *testing.T) {
	_, err := Open("oracle", "dsn")
	require.Error(t, err)
	assert.Contains(t, err.Error(), `Unknown driver "oracle"`)
}

func TestOpenBadPostgresDSN(t *testing.T) {
	// fails at sql.Open: pgx parses the DSN through the connector
	_, err := Open("postgres", "=%=not-a-dsn")
	assert.Error(t, err)
}

func TestIsClosed(t *testing.T) {
	var nilDB *db
	assert.True(t, nilDB.IsClosed(), "nil handle counts as closed")

	d, _ := newFakeSqlPro(t, SQLITE3)
	assert.False(t, d.IsClosed())
	require.NoError(t, d.Close())
	assert.True(t, d.IsClosed())
}

func TestClosePanics(t *testing.T) {
	assert.Panics(t, func() {
		_ = pureDB().Close()
	}, "Close without Open")

	d, _ := newFakeSqlPro(t, SQLITE3)
	tx, err := d.Begin()
	require.NoError(t, err)
	defer func() { _ = tx.Rollback() }()
	assert.Panics(t, func() {
		_ = tx.(*db).Close()
	}, "Close on a TX handle")
}

// --- small helpers -----------------------------------------------------------------

func TestEscHelpers(t *testing.T) {
	d := pureDB()
	assert.Equal(t, `"co""l"`, d.Esc(`co"l`))
	assert.Equal(t, `'it''s'`, d.EscValue("it's"))
}

func TestDriverAccessors(t *testing.T) {
	d, _ := newFakeSqlPro(t, POSTGRES)
	assert.Equal(t, dbDriver(POSTGRES), d.Driver())
	assert.NotNil(t, d.DB())
	assert.Contains(t, d.String(), "postgres")
}

func typeOf(v any) reflect.Type { return reflect.TypeOf(v) }
