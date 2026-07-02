package sqlpro

import (
	"database/sql/driver"
	"testing"
)

// benchFakeValueRow is shaped like fylr's hottest bulk-scanned struct
// (object.Value): 14 db columns, mostly pointer fields with plenty of NULLs,
// read as []*T. The fake driver keeps driver noise out of the numbers,
// unlike the SQLite-backed BenchmarkScanRows.
type benchFakeValueRow struct {
	ID       int64   `db:"id,pk,omitempty"`
	ObjectID *int64  `db:"object_id"`
	ParentID *int64  `db:"parent_id"`
	ColumnID *int64  `db:"column_id"`
	Position int64   `db:"position"`
	Kind     int64   `db:"kind"`
	Text     *string `db:"text"`
	Number   *int64  `db:"number"`
	UUID     *string `db:"uuid"`
	Lang     *string `db:"lang"`
	FileID   *int64  `db:"file_id"`
	LinkID   *int64  `db:"link_id"`
	Sort     *string `db:"sort"`
	Comment  *string `db:"comment"`
}

func benchFakeValueRows(n int) (cols []string, rows [][]driver.Value) {
	cols = []string{
		"id", "object_id", "parent_id", "column_id", "position", "kind",
		"text", "number", "uuid", "lang", "file_id", "link_id", "sort",
		"comment",
	}
	rows = make([][]driver.Value, 0, n)
	for k := 0; k < n; k++ {
		oid := int64(k / 10)
		colID := int64(k % 7)
		row := []driver.Value{
			int64(k + 1), oid, nil, colID, int64(k % 5), int64(2),
			"some text value for the row", nil,
			"9f7e0e2e-0000-0000-0000-000000000000", "de-DE", nil, nil,
			"736f6d6520736f7274", nil,
		}
		rows = append(rows, row)
	}
	return cols, rows
}

func BenchmarkFakeScanPtrRows(b *testing.B) {
	const n = 2000
	wrapper, backend := newFakeSqlPro(b, POSTGRES)
	cols, rows := benchFakeValueRows(n)
	for i := 0; i < b.N; i++ {
		backend.queueQuery(cols, rows)
	}

	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		var out []*benchFakeValueRow
		if err := wrapper.Query(&out, "SELECT * FROM value"); err != nil {
			b.Fatal(err)
		}
		if len(out) != n {
			b.Fatalf("expected %d rows, got %d", n, len(out))
		}
	}
}
