package sqlpro

// A list too long to bind one placeholder per item used to be inlined as
// literals, which stops parsing on both engines once it gets long enough
// (postgres: "invalid memory alloc request size 1073741824" past 2^23 items;
// sqlite: "out of memory"). Behind IN / NOT IN the list now travels as one
// bound argument instead. See #80845.

import (
	"fmt"
	"net/url"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func inListDB(t *testing.T, driver dbDriver) *db {
	t.Helper()
	db2 := newSqlPro(nil)
	db2.driver = driver
	if driver == POSTGRES {
		db2.PlaceholderMode = DOLLAR
	}
	db2.MaxPlaceholder = 3
	return db2
}

func longIDs(n int) []int64 {
	ids := make([]int64, n)
	for i := range ids {
		ids[i] = int64(i + 1)
	}
	return ids
}

func TestInListBoundPostgres(t *testing.T) {
	db2 := inListDB(t, POSTGRES)

	for _, tc := range []struct {
		name   string
		sql    string
		expSQL string
	}{
		{"IN", `SELECT * FROM "t" WHERE "id" IN ?`, `SELECT * FROM "t" WHERE "id" = ANY($1)`},
		{"NOT IN", `SELECT * FROM "t" WHERE "id" NOT IN ?`, `SELECT * FROM "t" WHERE "id" <> ALL($1)`},
		{"lower case", `SELECT * FROM "t" WHERE "id" not in ?`, `SELECT * FROM "t" WHERE "id" <> ALL($1)`},
		{"newline before the list", "SELECT * FROM \"t\"\n WHERE \"id\" IN\n\t?", "SELECT * FROM \"t\"\n WHERE \"id\" = ANY($1)"},
		{"after an opening paren", `SELECT * FROM "t" WHERE ("id" IN ?)`, `SELECT * FROM "t" WHERE ("id" = ANY($1))`},
	} {
		t.Run(tc.name, func(t *testing.T) {
			sqlS, args, err := db2.replaceArgs(tc.sql, longIDs(4))
			require.NoError(t, err)
			assert.Equal(t, tc.expSQL, sqlS)
			require.Len(t, args, 1)
			assert.Equal(t, longIDs(4), args[0])
		})
	}
}

func TestInListBoundSqlite(t *testing.T) {
	db2 := inListDB(t, SQLITE3)

	sqlS, args, err := db2.replaceArgs(`SELECT * FROM "t" WHERE "id" IN ?`, longIDs(4))
	require.NoError(t, err)
	assert.Equal(t, `SELECT * FROM "t" WHERE "id" IN (SELECT "value" FROM json_each(?))`, sqlS)
	require.Len(t, args, 1)
	assert.Equal(t, `[1,2,3,4]`, args[0])

	// NOT IN keeps its keyword, only the set is rewritten
	sqlS, _, err = db2.replaceArgs(`SELECT * FROM "t" WHERE "id" NOT IN ?`, longIDs(4))
	require.NoError(t, err)
	assert.Equal(t, `SELECT * FROM "t" WHERE "id" NOT IN (SELECT "value" FROM json_each(?))`, sqlS)

	sqlS, args, err = db2.replaceArgs(`SELECT * FROM "t" WHERE "name" IN ?`, []string{"a", "b", `c"d`, "e"})
	require.NoError(t, err)
	assert.Equal(t, `SELECT * FROM "t" WHERE "name" IN (SELECT "value" FROM json_each(?))`, sqlS)
	assert.Equal(t, `["a","b","c\"d","e"]`, args[0])
}

// Short lists are unaffected: one placeholder per item, as before.
func TestInListShortUnchanged(t *testing.T) {
	for _, driver := range []dbDriver{POSTGRES, SQLITE3} {
		db2 := inListDB(t, driver)
		sqlS, args, err := db2.replaceArgs(`SELECT * FROM "t" WHERE "id" IN ?`, longIDs(3))
		require.NoError(t, err)
		assert.Contains(t, sqlS, "IN (")
		assert.NotContains(t, sqlS, "ANY")
		assert.NotContains(t, sqlS, "json_each")
		assert.Len(t, args, 3)
	}
}

// Outside an IN the parenthesised literal list is the only thing that fits,
// so those keep inlining.
func TestInListNonInContextInlines(t *testing.T) {
	db2 := inListDB(t, POSTGRES)

	for _, sqlS := range []string{
		`INSERT INTO "t" ("a","b","c","d") VALUES ?`,
		`SELECT * FROM "t" WHERE "fin" = ANY(?)`,
		// "fin" ends in "in" but is an identifier, not the keyword
		`SELECT * FROM "t" WHERE fin ?`,
	} {
		out, args, err := db2.replaceArgs(sqlS, longIDs(4))
		require.NoError(t, err)
		assert.Contains(t, out, "(1,2,3,4)", sqlS)
		assert.Empty(t, args, sqlS)
	}
}

// A quoted identifier or a string ending in "in" must not be mistaken for the
// keyword.
func TestInListQuotedIdentifierNotKeyword(t *testing.T) {
	db2 := inListDB(t, POSTGRES)

	out, args, err := db2.replaceArgs(`SELECT * FROM "t" WHERE "in" ?`, longIDs(4))
	require.NoError(t, err)
	assert.Contains(t, out, "(1,2,3,4)")
	assert.Empty(t, args)
}

// Types the bound path cannot carry fall back to literals rather than failing.
func TestInListUnsupportedElementFallsBack(t *testing.T) {
	db2 := inListDB(t, POSTGRES)

	out, args, err := db2.replaceArgs(`SELECT * FROM "t" WHERE "f" IN ?`, []float64{1, 2, 3, 4})
	assert.Error(t, err, "float elements are still rejected as before: %s %v", out, args)
}

// Pointer elements keep their NULLs.
func TestInListPointerElements(t *testing.T) {
	db2 := inListDB(t, POSTGRES)

	one, three := int64(1), int64(3)
	out, args, err := db2.replaceArgs(`SELECT * FROM "t" WHERE "id" IN ?`, []*int64{&one, nil, &three, nil})
	require.NoError(t, err)
	assert.Equal(t, `SELECT * FROM "t" WHERE "id" = ANY($1)`, out)
	require.Len(t, args, 1)
	ptrs, ok := args[0].([]*int64)
	require.True(t, ok)
	require.Len(t, ptrs, 4)
	assert.Nil(t, ptrs[1])
	assert.Equal(t, int64(3), *ptrs[2])
}

// The statement text no longer grows with the list.
func TestInListStatementStaysShort(t *testing.T) {
	db2 := inListDB(t, POSTGRES)

	base := `SELECT * FROM "object_tag" WHERE "object_id" IN ?`
	out, _, err := db2.replaceArgs(base, longIDs(1_000_000))
	require.NoError(t, err)
	assert.Less(t, len(out), len(base)+16)
	assert.False(t, strings.Contains(out, "999999"))
}

// The rewritten forms must actually run on the engines, not just look right:
// json_each has to be compiled into the sqlite driver we ship with, and the
// bound array has to survive database/sql on postgres.
func TestInListRoundTripSqlite(t *testing.T) {
	qv := url.Values{}
	qv.Add("_pragma", "busy_timeout(1000)")
	d, err := Open("sqlite", filepath.Join(t.TempDir(), "in_list.db")+"?"+qv.Encode())
	require.NoError(t, err)
	defer d.Close()

	require.NoError(t, d.Exec(`CREATE TABLE t(id INTEGER, name TEXT)`))
	for i := 1; i <= 300; i++ {
		require.NoError(t, d.Exec(`INSERT INTO t VALUES(?, ?)`, i, fmt.Sprintf("n%d", i)))
	}

	// well past MaxPlaceholder (100), so the bound path is taken
	ids := longIDs(250)
	var n int
	require.NoError(t, d.Query(&n, `SELECT count(*) FROM t WHERE "id" IN ?`, ids))
	assert.Equal(t, 250, n)

	require.NoError(t, d.Query(&n, `SELECT count(*) FROM t WHERE "id" NOT IN ?`, ids))
	assert.Equal(t, 50, n)

	names := make([]string, 0, 250)
	for i := 1; i <= 250; i++ {
		names = append(names, fmt.Sprintf("n%d", i))
	}
	require.NoError(t, d.Query(&n, `SELECT count(*) FROM t WHERE "name" IN ?`, names))
	assert.Equal(t, 250, n)

	// a list far beyond what a literal list can be parsed as
	huge := longIDs(2_000_000)
	require.NoError(t, d.Query(&n, `SELECT count(*) FROM t WHERE "id" IN ?`, huge))
	assert.Equal(t, 300, n)
}

func TestInListRoundTripPostgres(t *testing.T) {
	d := openPG(t)
	defer d.Close()

	require.NoError(t, d.Exec(`DROP TABLE IF EXISTS sqlpro_in_list_t`))
	require.NoError(t, d.Exec(`CREATE TABLE sqlpro_in_list_t(id BIGINT, name TEXT)`))
	defer d.Exec(`DROP TABLE IF EXISTS sqlpro_in_list_t`)
	require.NoError(t, d.Exec(
		`INSERT INTO sqlpro_in_list_t SELECT g, 'n' || g FROM generate_series(1,300) g`))

	ids := longIDs(250)
	var n int
	require.NoError(t, d.Query(&n, `SELECT count(*) FROM sqlpro_in_list_t WHERE "id" IN ?`, ids))
	assert.Equal(t, 250, n)

	require.NoError(t, d.Query(&n, `SELECT count(*) FROM sqlpro_in_list_t WHERE "id" NOT IN ?`, ids))
	assert.Equal(t, 50, n)

	names := make([]string, 0, 250)
	for i := 1; i <= 250; i++ {
		names = append(names, fmt.Sprintf("n%d", i))
	}
	require.NoError(t, d.Query(&n, `SELECT count(*) FROM sqlpro_in_list_t WHERE "name" IN ?`, names))
	assert.Equal(t, 250, n)

	// 10M ids: a literal list of this length fails to parse with
	// "invalid memory alloc request size 1073741824"
	require.NoError(t, d.Query(&n, `SELECT count(*) FROM sqlpro_in_list_t WHERE "id" IN ?`, longIDs(10_000_000)))
	assert.Equal(t, 300, n)
}

// A bound list must not reappear in the log as an argument dump.
func TestArgsToStringSummarisesLongSlices(t *testing.T) {
	out := argsToString(longIDs(8_400_000))
	assert.Contains(t, out, "[1,2,3,4,5,6,7,8,9,10,... 8400000 items]")
	assert.Less(t, len(out), 100)

	// short lists are still printed in full, and []byte stays a value
	assert.Contains(t, argsToString([]int64{1, 2, 3}), "[1 2 3]")
	assert.NotContains(t, argsToString([]byte("hello")), "items]")
}
