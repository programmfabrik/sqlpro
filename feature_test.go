package sqlpro

// feature_test.go is a from-scratch, feature-by-feature test suite that aims to
// cover the full public surface of sqlpro. It is independent of the older tests
// in query_test.go etc. and uses its own tables (prefix "feat_"). It runs
// against the SQLite database set up in TestMain.

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func mustExec(t *testing.T, sql string) {
	t.Helper()
	require.NoError(t, dbConn.Exec(sql))
}

// --- struct tags: pk, omitempty, readonly, "-", embedding -------------------

func TestFeatureTags(t *testing.T) {
	mustExec(t, `DROP TABLE IF EXISTS feat_tags`)
	mustExec(t, `CREATE TABLE feat_tags(
		id     INTEGER PRIMARY KEY AUTOINCREMENT,
		source TEXT NOT NULL DEFAULT 'embedded-default',
		name   TEXT NOT NULL,
		kind   TEXT NOT NULL DEFAULT 'db-default',
		slug   TEXT NOT NULL DEFAULT 'readonly-default')`)

	type meta struct {
		Source string `db:"source,omitempty"`
	}
	type row struct {
		meta         // embedded: its "source" column is promoted
		ID    int64  `db:"id,pk,omitempty"`
		Name  string `db:"name"`
		Kind  string `db:"kind,omitempty"` // zero -> DB default applies
		Slug  string `db:"slug,readonly"`  // never written
		Notes string `db:"-"`              // never persisted
	}

	in := &row{Name: "widget", Slug: "should-be-ignored", Notes: "go-only"}
	require.NoError(t, dbConn.Insert("feat_tags", in))
	assert.Greater(t, in.ID, int64(0), "pk written back")

	var got row
	require.NoError(t, dbConn.Query(&got, "SELECT * FROM feat_tags WHERE id = ?", in.ID))
	assert.Equal(t, "widget", got.Name)
	assert.Equal(t, "embedded-default", got.Source, "omitempty embedded field -> DB default")
	assert.Equal(t, "db-default", got.Kind, "omitempty field -> DB default")
	assert.Equal(t, "readonly-default", got.Slug, "readonly field not written -> DB default")
	assert.Equal(t, "", got.Notes, `db:"-" never persisted`)
}

// --- json column + nullable pointers ----------------------------------------

func TestFeatureJSONAndNull(t *testing.T) {
	mustExec(t, `DROP TABLE IF EXISTS feat_json`)
	mustExec(t, `CREATE TABLE feat_json(
		id  INTEGER PRIMARY KEY AUTOINCREMENT,
		opt TEXT,
		cfg TEXT)`)

	type cfg struct {
		Theme string `json:"theme"`
	}
	type row struct {
		ID  int64   `db:"id,pk,omitempty"`
		Opt *string `db:"opt"`      // nullable
		Cfg *cfg    `db:"cfg,json"` // JSON, nil -> NULL
	}

	opt := "set"
	require.NoError(t, dbConn.Insert("feat_json", &row{Opt: &opt, Cfg: &cfg{Theme: "dark"}}))
	require.NoError(t, dbConn.Insert("feat_json", &row{})) // Opt nil, Cfg nil

	var rows []*row
	require.NoError(t, dbConn.Query(&rows, "SELECT * FROM feat_json ORDER BY id"))
	require.Len(t, rows, 2)

	require.NotNil(t, rows[0].Opt)
	assert.Equal(t, "set", *rows[0].Opt)
	require.NotNil(t, rows[0].Cfg)
	assert.Equal(t, "dark", rows[0].Cfg.Theme)

	assert.Nil(t, rows[1].Opt, "nil pointer round-trips as NULL")
	assert.Nil(t, rows[1].Cfg, "zero JSON pointer stored as NULL")

	// Confirm the second row really is SQL NULL, not the string "null".
	var n int64
	require.NoError(t, dbConn.Query(&n, "SELECT count(*) FROM feat_json WHERE cfg IS NULL"))
	assert.Equal(t, int64(1), n)
}

// --- Save = upsert (insert when pk zero, update when set) --------------------

func TestFeatureSaveUpsert(t *testing.T) {
	mustExec(t, `DROP TABLE IF EXISTS feat_save`)
	mustExec(t, `CREATE TABLE feat_save(id INTEGER PRIMARY KEY AUTOINCREMENT, v TEXT)`)

	type row struct {
		ID int64  `db:"id,pk,omitempty"`
		V  string `db:"v"`
	}

	r := &row{V: "first"}
	require.NoError(t, dbConn.Save("feat_save", r)) // insert path
	require.Greater(t, r.ID, int64(0))

	r.V = "second"
	require.NoError(t, dbConn.Save("feat_save", r)) // update path (pk set)

	var got row
	require.NoError(t, dbConn.Query(&got, "SELECT * FROM feat_save WHERE id = ?", r.ID))
	assert.Equal(t, "second", got.V)

	var count int64
	require.NoError(t, dbConn.Query(&count, "SELECT count(*) FROM feat_save"))
	assert.Equal(t, int64(1), count, "upsert did not create a duplicate")
}

// --- bulk: InsertBulk, OnConflictDoNothing, UpdateBulk ----------------------

func TestFeatureBulk(t *testing.T) {
	ctx := context.Background()
	mustExec(t, `DROP TABLE IF EXISTS feat_bulk`)
	mustExec(t, `CREATE TABLE feat_bulk(
		id   INTEGER PRIMARY KEY AUTOINCREMENT,
		name TEXT NOT NULL UNIQUE,
		n    INTEGER)`)

	type row struct {
		ID   int64  `db:"id,pk,omitempty"`
		Name string `db:"name"`
		N    int64  `db:"n"`
	}

	require.NoError(t, dbConn.InsertBulk("feat_bulk", []*row{
		{Name: "a", N: 1}, {Name: "b", N: 2}, {Name: "c", N: 3},
	}))

	// OnConflictDoNothing on the unique "name": "a" is skipped, "d" inserted.
	require.NoError(t, dbConn.InsertBulkOnConflictDoNothingContext(ctx, "feat_bulk",
		[]*row{{Name: "a", N: 99}, {Name: "d", N: 4}}, "name"))

	var total int64
	require.NoError(t, dbConn.Query(&total, "SELECT count(*) FROM feat_bulk"))
	assert.Equal(t, int64(4), total)

	var aN int64
	require.NoError(t, dbConn.Query(&aN, "SELECT n FROM feat_bulk WHERE name = 'a'"))
	assert.Equal(t, int64(1), aN, "conflict row was not overwritten")

	// UpdateBulk by pk.
	var all []*row
	require.NoError(t, dbConn.Query(&all, "SELECT * FROM feat_bulk ORDER BY id"))
	for _, r := range all {
		r.N += 100
	}
	require.NoError(t, dbConn.UpdateBulkContext(ctx, "feat_bulk", all))

	var sum int64
	require.NoError(t, dbConn.Query(&sum, "SELECT sum(n) FROM feat_bulk"))
	assert.Equal(t, int64(1+2+3+4+400), sum)
}

// --- transactions: ExecTX commit/rollback -----------------------------------

func TestFeatureExecTX(t *testing.T) {
	ctx := context.Background()
	mustExec(t, `DROP TABLE IF EXISTS feat_tx`)
	mustExec(t, `CREATE TABLE feat_tx(id INTEGER PRIMARY KEY AUTOINCREMENT, v TEXT)`)

	type row struct {
		ID int64  `db:"id,pk,omitempty"`
		V  string `db:"v"`
	}

	// Commit: writes are visible afterwards, and operations inside use CtxTX.
	require.NoError(t, dbConn.ExecTX(ctx, func(ctx context.Context) error {
		tx := CtxTX(ctx)
		assert.True(t, tx.ActiveTX())
		assert.True(t, tx.IsWriteMode())
		return tx.Insert("feat_tx", &row{V: "kept"})
	}, nil))

	// Rollback: returning an error discards all writes in the job.
	sentinel := errors.New("boom")
	err := dbConn.ExecTX(ctx, func(ctx context.Context) error {
		tx := CtxTX(ctx)
		if err := tx.Insert("feat_tx", &row{V: "discarded"}); err != nil {
			return err
		}
		return sentinel
	}, nil)
	assert.ErrorIs(t, err, sentinel)

	var vals []string
	require.NoError(t, dbConn.Query(&vals, "SELECT v FROM feat_tx ORDER BY id"))
	assert.Equal(t, []string{"kept"}, vals)
}

// --- transactions: nesting is rejected --------------------------------------

func TestFeatureExecTXNoNesting(t *testing.T) {
	ctx := context.Background()
	err := dbConn.ExecTX(ctx, func(ctx context.Context) error {
		return dbConn.ExecTX(ctx, func(ctx context.Context) error { return nil }, nil)
	}, nil)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "unable to nest")
}

// --- transactions: hooks ----------------------------------------------------

func TestFeatureTXHooks(t *testing.T) {
	ctx := context.Background()
	mustExec(t, `DROP TABLE IF EXISTS feat_hooks`)
	mustExec(t, `CREATE TABLE feat_hooks(id INTEGER PRIMARY KEY AUTOINCREMENT, v TEXT)`)

	type row struct {
		ID int64  `db:"id,pk,omitempty"`
		V  string `db:"v"`
	}

	// Commit case: BeforeCommit, AfterCommit and AfterTransaction fire;
	// AfterRollback does not.
	var order []string
	require.NoError(t, dbConn.ExecTX(ctx, func(ctx context.Context) error {
		tx := CtxTX(ctx)
		tx.BeforeCommit(func() error { order = append(order, "before"); return nil })
		tx.AfterCommit(func() { order = append(order, "afterCommit") })
		tx.AfterRollback(func() { order = append(order, "afterRollback") })
		tx.AfterTransaction(func() { order = append(order, "afterTx") })
		return tx.Insert("feat_hooks", &row{V: "x"})
	}, nil))
	assert.Equal(t, []string{"before", "afterCommit", "afterTx"}, order)

	// A failing BeforeCommit hook rolls the transaction back.
	hookErr := errors.New("veto")
	err := dbConn.ExecTX(ctx, func(ctx context.Context) error {
		tx := CtxTX(ctx)
		tx.BeforeCommit(func() error { return hookErr })
		return tx.Insert("feat_hooks", &row{V: "vetoed"})
	}, nil)
	assert.ErrorIs(t, err, hookErr)

	var count int64
	require.NoError(t, dbConn.Query(&count, "SELECT count(*) FROM feat_hooks"))
	assert.Equal(t, int64(1), count, "vetoed insert was rolled back")

	// Rollback case: AfterRollback and AfterTransaction fire; the commit hooks do not.
	order = nil
	sentinel := errors.New("rollback me")
	_ = dbConn.ExecTX(ctx, func(ctx context.Context) error {
		tx := CtxTX(ctx)
		tx.BeforeCommit(func() error { order = append(order, "before"); return nil })
		tx.AfterCommit(func() { order = append(order, "afterCommit") })
		tx.AfterRollback(func() { order = append(order, "afterRollback") })
		tx.AfterTransaction(func() { order = append(order, "afterTx") })
		return sentinel
	}, nil)
	assert.Equal(t, []string{"afterRollback", "afterTx"}, order)
}

// --- Exec helpers -----------------------------------------------------------

func TestFeatureExecRowsAffected(t *testing.T) {
	ctx := context.Background()
	mustExec(t, `DROP TABLE IF EXISTS feat_exec`)
	mustExec(t, `CREATE TABLE feat_exec(id INTEGER PRIMARY KEY AUTOINCREMENT, v INTEGER)`)

	type row struct {
		ID int64 `db:"id,pk,omitempty"`
		V  int64 `db:"v"`
	}
	require.NoError(t, dbConn.InsertBulk("feat_exec", []*row{{V: 1}, {V: 1}, {V: 2}}))

	affected, _, err := dbConn.ExecContextRowsAffected(ctx, "UPDATE feat_exec SET v = 9 WHERE v = 1")
	require.NoError(t, err)
	assert.Equal(t, int64(2), affected)

	// Empty SQL is rejected.
	assert.Error(t, dbConn.Exec(""))
}

// --- query: zero rows -------------------------------------------------------

func TestFeatureZeroRows(t *testing.T) {
	mustExec(t, `DROP TABLE IF EXISTS feat_zero`)
	mustExec(t, `CREATE TABLE feat_zero(id INTEGER PRIMARY KEY)`)

	var x int64
	err := dbConn.Query(&x, "SELECT id FROM feat_zero WHERE id = 1")
	assert.ErrorIs(t, err, ErrQueryReturnedZeroRows)

	// A slice target just comes back empty (no error).
	var xs []int64
	assert.NoError(t, dbConn.Query(&xs, "SELECT id FROM feat_zero"))
	assert.Empty(t, xs)
}

// --- placeholders: IN ? expansion and @ identifier --------------------------

func TestFeaturePlaceholders(t *testing.T) {
	mustExec(t, `DROP TABLE IF EXISTS feat_ph`)
	mustExec(t, `CREATE TABLE feat_ph(id INTEGER PRIMARY KEY AUTOINCREMENT, name TEXT)`)

	type row struct {
		ID   int64  `db:"id,pk,omitempty"`
		Name string `db:"name"`
	}
	require.NoError(t, dbConn.InsertBulk("feat_ph", []*row{
		{Name: "x"}, {Name: "y"}, {Name: "z"},
	}))

	// IN ? expands the slice into the right number of placeholders.
	var names []string
	require.NoError(t, dbConn.Query(&names,
		"SELECT name FROM feat_ph WHERE name IN ? ORDER BY name", []string{"x", "z", "q"}))
	assert.Equal(t, []string{"x", "z"}, names)

	// @ quotes its argument as a SQL identifier (table/column name).
	var count int64
	require.NoError(t, dbConn.Query(&count, "SELECT count(*) FROM @", "feat_ph"))
	assert.Equal(t, int64(3), count)
}

// --- escaping + ILIKE helpers -----------------------------------------------

func TestFeatureEscaping(t *testing.T) {
	assert.Equal(t, `'O''Hara'`, escValue("O'Hara"))
	assert.Equal(t, `'O''Hara'`, dbConn.EscValue("O'Hara"))

	// IlikeSql builds a driver-correct snippet (SQLite here).
	snippet := IlikeSql(dbConn.Driver(), "berg")
	assert.Contains(t, snippet, "LIKE")
	assert.Contains(t, snippet, "%berg%")
}

// --- introspection ----------------------------------------------------------

func TestFeatureVersionName(t *testing.T) {
	v, err := dbConn.Version()
	require.NoError(t, err)
	assert.Contains(t, v, "Sqlite")

	n, err := dbConn.Name()
	require.NoError(t, err)
	assert.NotEmpty(t, n)
}

// --- NullTime / NullJson / NullRawMessage scanners (read path) ---------------

func TestFeatureNullScanners(t *testing.T) {
	mustExec(t, `DROP TABLE IF EXISTS feat_null`)
	mustExec(t, `CREATE TABLE feat_null(id INTEGER PRIMARY KEY AUTOINCREMENT, t DATETIME, j TEXT, r TEXT)`)

	now := time.Now()
	type row struct {
		ID int64            `db:"id,pk,omitempty"`
		T  *time.Time       `db:"t"`
		J  json.RawMessage  `db:"j"`
		R  *json.RawMessage `db:"r"`
	}
	raw := json.RawMessage(`{"a":1}`)
	require.NoError(t, dbConn.Insert("feat_null", &row{T: &now, J: json.RawMessage(`[1,2]`), R: &raw}))
	require.NoError(t, dbConn.Insert("feat_null", &row{})) // all NULL

	var rows []*row
	require.NoError(t, dbConn.Query(&rows, "SELECT * FROM feat_null ORDER BY id"))
	require.Len(t, rows, 2)

	require.NotNil(t, rows[0].T)
	assert.Equal(t, now.Format(time.RFC3339Nano), rows[0].T.Format(time.RFC3339Nano))
	assert.JSONEq(t, `[1,2]`, string(rows[0].J))
	require.NotNil(t, rows[0].R)
	assert.JSONEq(t, `{"a":1}`, string(*rows[0].R))

	assert.Nil(t, rows[1].T)
	assert.Empty(t, rows[1].J)
	assert.Nil(t, rows[1].R)
}

// --- null / notnull tag options (non-json fields) ---------------------------

func TestFeatureNullNotNull(t *testing.T) {
	mustExec(t, `DROP TABLE IF EXISTS feat_nn`)
	mustExec(t, `CREATE TABLE feat_nn(id INTEGER PRIMARY KEY AUTOINCREMENT, plain INTEGER, nullable INTEGER)`)

	// "null" on a value field stores a zero value as SQL NULL; without it, the
	// zero value (0) is stored.
	type row struct {
		ID       int64 `db:"id,pk,omitempty"`
		Plain    int64 `db:"plain"`         // zero -> 0
		Nullable int64 `db:"nullable,null"` // zero -> NULL
	}
	require.NoError(t, dbConn.Insert("feat_nn", &row{}))

	var plainNull, nullableNull bool
	require.NoError(t, dbConn.Query(&plainNull, "SELECT plain IS NULL FROM feat_nn"))
	require.NoError(t, dbConn.Query(&nullableNull, "SELECT nullable IS NULL FROM feat_nn"))
	assert.False(t, plainNull, "plain zero stored as 0, not NULL")
	assert.True(t, nullableNull, `"null" tag stores zero as SQL NULL`)

	// "notnull" on a pointer field rejects a nil value (it would otherwise become
	// NULL): storing nil panics.
	type strict struct {
		ID   int64   `db:"id,pk,omitempty"`
		Name *string `db:"plain,notnull"`
	}
	assert.Panics(t, func() {
		_ = dbConn.Insert("feat_nn", &strict{}) // Name is nil + notnull -> panic
	})
}

// --- json null handling: null / notnull / default ---------------------------

func TestFeatureJSONNullModes(t *testing.T) {
	mustExec(t, `DROP TABLE IF EXISTS feat_jnull`)
	mustExec(t, `CREATE TABLE feat_jnull(id INTEGER PRIMARY KEY AUTOINCREMENT, jdef TEXT, jnull TEXT, jnotnull TEXT)`)

	type cfg struct {
		A int `json:"a"`
	}
	// All three pointers are nil. A nil pointer marshals to "null".
	type row struct {
		ID       int64 `db:"id,pk,omitempty"`
		JDef     *cfg  `db:"jdef,json"`             // default: "null" -> SQL NULL
		JNull    *cfg  `db:"jnull,json,null"`       // null:    -> SQL NULL
		JNotNull *cfg  `db:"jnotnull,json,notnull"` // notnull: -> literal "null"
	}
	require.NoError(t, dbConn.Insert("feat_jnull", &row{}))

	var defNull, nullNull, notnullNull bool
	require.NoError(t, dbConn.Query(&defNull, "SELECT jdef IS NULL FROM feat_jnull"))
	require.NoError(t, dbConn.Query(&nullNull, "SELECT jnull IS NULL FROM feat_jnull"))
	require.NoError(t, dbConn.Query(&notnullNull, "SELECT jnotnull IS NULL FROM feat_jnull"))

	assert.True(t, defNull, `default json: zero marshaling to "null" -> SQL NULL`)
	assert.True(t, nullNull, `",null": zero -> SQL NULL`)
	assert.False(t, notnullNull, `",notnull": zero -> literal "null", not SQL NULL`)

	var raw string
	require.NoError(t, dbConn.Query(&raw, "SELECT jnotnull FROM feat_jnull"))
	assert.Equal(t, "null", raw, `",notnull" stores the JSON text "null"`)
}

// --- json_ignore_error on read ----------------------------------------------

func TestFeatureJSONIgnoreError(t *testing.T) {
	mustExec(t, `DROP TABLE IF EXISTS feat_jerr`)
	mustExec(t, `CREATE TABLE feat_jerr(id INTEGER PRIMARY KEY AUTOINCREMENT, j TEXT)`)
	// Put invalid JSON into the column directly.
	mustExec(t, `INSERT INTO feat_jerr(j) VALUES ('not json at all')`)

	type cfg struct {
		A int `json:"a"`
	}

	// Without the flag, reading the invalid JSON is an error.
	type strict struct {
		ID int64 `db:"id,pk,omitempty"`
		J  cfg   `db:"j,json"`
	}
	var s strict
	assert.Error(t, dbConn.Query(&s, "SELECT * FROM feat_jerr"))

	// With json_ignore_error, the unmarshal error is swallowed and the field is
	// left zero.
	type lenient struct {
		ID int64 `db:"id,pk,omitempty"`
		J  cfg   `db:"j,json,json_ignore_error"`
	}
	var l lenient
	assert.NoError(t, dbConn.Query(&l, "SELECT * FROM feat_jerr"))
	assert.Equal(t, cfg{}, l.J)
}
