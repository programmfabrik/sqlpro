package sqlpro

import (
	"context"
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
)

type bulkReadBackRow struct {
	ID   int64  `db:"id,pk,omitempty"`
	Name string `db:"name"`
}

// TestInsertBulkReadbackIds checks InsertBulkReadbackIdsContext against the
// real database (SQLite here): the generated keys must be assigned in row
// order, and the plain InsertBulk must NOT read them back.
func TestInsertBulkReadbackIds(t *testing.T) {
	ctx := context.Background()
	require.NoError(t, dbConn.Exec(`DROP TABLE IF EXISTS bulk_readback`))
	require.NoError(t, dbConn.Exec(`CREATE TABLE bulk_readback(
		id   INTEGER PRIMARY KEY AUTOINCREMENT,
		name TEXT NOT NULL UNIQUE)`))

	n := 1500
	rows := make([]*bulkReadBackRow, 0, n)
	for i := 0; i < n; i++ {
		rows = append(rows, &bulkReadBackRow{Name: fmt.Sprintf("row %d", i)})
	}
	require.NoError(t, dbConn.InsertBulkReadbackIdsContext(ctx, "bulk_readback", rows))

	for i, row := range rows {
		require.Greater(t, row.ID, int64(0), "row %d has no id", i)
		if i > 0 {
			require.Greater(t, row.ID, rows[i-1].ID, "ids not ascending at row %d", i)
		}
	}

	// the keys must map to the right rows: read back and compare content
	got := []*bulkReadBackRow{}
	require.NoError(t, dbConn.Query(&got, `SELECT * FROM bulk_readback ORDER BY "id"`))
	require.Equal(t, n, len(got))
	for i, g := range got {
		require.Equal(t, rows[i].ID, g.ID)
		require.Equal(t, fmt.Sprintf("row %d", i), g.Name)
	}

	// plain InsertBulk inserts but does NOT read the keys back
	plain := []*bulkReadBackRow{{Name: "plain a"}, {Name: "plain b"}}
	require.NoError(t, dbConn.InsertBulkContext(ctx, "bulk_readback", plain))
	require.Equal(t, int64(0), plain[0].ID, "InsertBulk must not read ids back")
	require.Equal(t, int64(0), plain[1].ID)

	// a pre-set primary key is rejected: the positional mapping would be wrong
	preset := []*bulkReadBackRow{{Name: "x"}, {ID: 99999, Name: "y"}}
	require.ErrorContains(t, dbConn.InsertBulkReadbackIdsContext(ctx, "bulk_readback", preset),
		"has its primary key")

	// empty slice is a no-op
	require.NoError(t, dbConn.InsertBulkReadbackIdsContext(ctx, "bulk_readback", []*bulkReadBackRow{}))

	require.NoError(t, dbConn.Exec(`DROP TABLE bulk_readback`))
}

// TestInsertBulkReadbackIdsRejects checks the struct-shape validation.
func TestInsertBulkReadbackIdsRejects(t *testing.T) {
	ctx := context.Background()

	// interface-boxed rows are not addressable, so the keys cannot be
	// written back
	type valRow struct {
		ID   int64  `db:"id,pk,omitempty"`
		Name string `db:"name"`
	}
	require.ErrorContains(t,
		dbConn.InsertBulkReadbackIdsContext(ctx, "t", []any{valRow{Name: "a"}}),
		"not settable")

	// primary key without omitempty
	type noOmit struct {
		ID   int64  `db:"id,pk"`
		Name string `db:"name"`
	}
	require.ErrorContains(t,
		dbConn.InsertBulkReadbackIdsContext(ctx, "t", []*noOmit{{Name: "a"}}),
		"omitempty")

	// no primary key at all
	type noPk struct {
		Name string `db:"name"`
	}
	require.ErrorContains(t,
		dbConn.InsertBulkReadbackIdsContext(ctx, "t", []*noPk{{Name: "a"}}),
		"exactly one primary key")

	// pointer primary key
	type ptrPk struct {
		ID   *int64 `db:"id,pk,omitempty"`
		Name string `db:"name"`
	}
	require.ErrorContains(t,
		dbConn.InsertBulkReadbackIdsContext(ctx, "t", []*ptrPk{{Name: "a"}}),
		"must not be a pointer")
}
