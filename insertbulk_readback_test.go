package sqlpro

import (
	"context"
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
)

type bulkReadBackRow struct {
	ID   int64   `db:"id,pk,omitempty"`
	Name string  `db:"name"`
	Note *string `db:"note"`
}

// TestInsertBulkReadBack checks the automatic primary-key read-back of the
// bulk inserts against the real database (SQLite here): when the batch
// qualifies, the generated keys must be assigned in row order; with ON
// CONFLICT DO NOTHING the assignment is per row (conflicted rows keep zero).
func TestInsertBulkReadBack(t *testing.T) {
	ctx := context.Background()
	require.NoError(t, dbConn.Exec(`DROP TABLE IF EXISTS bulk_readback`))
	require.NoError(t, dbConn.Exec(`CREATE TABLE bulk_readback(
		id   INTEGER PRIMARY KEY AUTOINCREMENT,
		name TEXT NOT NULL UNIQUE,
		note TEXT)`))

	n := 1500
	rows := make([]*bulkReadBackRow, 0, n)
	for i := 0; i < n; i++ {
		rows = append(rows, &bulkReadBackRow{Name: fmt.Sprintf("row %d", i)})
	}
	require.NoError(t, dbConn.InsertBulkContext(ctx, "bulk_readback", rows))

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

	// a batch with a pre-set primary key does not qualify: it inserts, but no
	// keys are read back (the positional mapping would be wrong)
	preset := []*bulkReadBackRow{{ID: 100000, Name: "preset a"}, {ID: 100001, Name: "preset b"}}
	require.NoError(t, dbConn.InsertBulkContext(ctx, "bulk_readback", preset))
	require.Equal(t, int64(100000), preset[0].ID)
	require.Equal(t, int64(100001), preset[1].ID)

	// ON CONFLICT DO NOTHING without conflicts: keys read back positionally
	fresh := []*bulkReadBackRow{{Name: "fresh a"}, {Name: "fresh b"}}
	require.NoError(t, dbConn.InsertBulkOnConflictDoNothingContext(ctx, "bulk_readback", fresh, "name"))
	require.Greater(t, fresh[0].ID, int64(0))
	require.Greater(t, fresh[1].ID, fresh[0].ID)

	// ON CONFLICT DO NOTHING with a conflict: per-row read-back via the
	// follow-up match on the conflict columns — the conflicted row keeps its
	// zero key, the inserted row gets its key
	mixed := []*bulkReadBackRow{{Name: "fresh a"}, {Name: "fresh c"}}
	require.NoError(t, dbConn.InsertBulkOnConflictDoNothingContext(ctx, "bulk_readback", mixed, "name"))
	require.Equal(t, int64(0), mixed[0].ID, "conflicted row keeps zero id")
	require.Greater(t, mixed[1].ID, int64(0), "inserted row gets its id")
	var cID int64
	require.NoError(t, dbConn.Query(&cID, `SELECT id FROM bulk_readback WHERE name = 'fresh c'`))
	require.Equal(t, cID, mixed[1].ID)

	// without a conflict target the match compares the full row (note the
	// NULL note matched with IS NULL)
	note := "with note"
	full := []*bulkReadBackRow{{Name: "fresh b"}, {Name: "fresh d", Note: &note}}
	require.NoError(t, dbConn.InsertBulkOnConflictDoNothingContext(ctx, "bulk_readback", full))
	require.Equal(t, int64(0), full[0].ID, "conflicted row keeps zero id")
	require.Greater(t, full[1].ID, int64(0), "inserted row gets its id")
	var dID int64
	require.NoError(t, dbConn.Query(&dID, `SELECT id FROM bulk_readback WHERE name = 'fresh d'`))
	require.Equal(t, dID, full[1].ID)

	var c int64
	require.NoError(t, dbConn.Query(&c, `SELECT count(*) FROM bulk_readback WHERE name LIKE 'fresh %'`))
	require.Equal(t, int64(4), c)

	// a slice of interface-boxed values is not settable: inserts, no read-back
	boxed := []any{bulkReadBackRow{Name: "boxed a"}, bulkReadBackRow{Name: "boxed b"}}
	require.NoError(t, dbConn.InsertBulkContext(ctx, "bulk_readback", boxed))

	// empty slice is a no-op
	require.NoError(t, dbConn.InsertBulkContext(ctx, "bulk_readback", []*bulkReadBackRow{}))

	require.NoError(t, dbConn.Exec(`DROP TABLE bulk_readback`))
}
