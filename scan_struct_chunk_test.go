package sqlpro

import (
	"database/sql/driver"
	"fmt"
	"runtime"
	"testing"
	"unsafe"
)

// chunkRow is a []*T scan target with a large field no column maps to, so the
// bytes allocated per scanned row are dominated by the struct itself.
type chunkRow struct {
	ID   int64   `db:"id,pk,omitempty"`
	Name *string `db:"name"`
	Pad  [2048]byte
}

func chunkRows(n int) (cols []string, rows [][]driver.Value) {
	cols = []string{"id", "name"}
	rows = make([][]driver.Value, 0, n)
	for k := 0; k < n; k++ {
		var name driver.Value
		if k%3 != 0 {
			name = fmt.Sprintf("name-%d", k)
		}
		rows = append(rows, []driver.Value{int64(k + 1), name})
	}
	return cols, rows
}

// TestScanStructSlicePtrRowCounts scans result sets around the chunk sizes
// into []*T: every row has its own struct with its own values.
func TestScanStructSlicePtrRowCounts(t *testing.T) {
	for _, n := range []int{0, 1, 2, 3, 4, 5, 7, 8, 9, 31, 32, 33, 63, 64, 65, 66, 127, 128, 129, 200, 1000} {
		t.Run(fmt.Sprintf("rows=%d", n), func(t *testing.T) {
			wrapper, backend := newFakeSqlPro(t, POSTGRES)
			backend.queueQuery(chunkRows(n))

			var out []*chunkRow
			if err := wrapper.Query(&out, "SELECT id, name FROM t"); err != nil {
				t.Fatal(err)
			}
			if len(out) != n {
				t.Fatalf("expected %d rows, got %d", n, len(out))
			}
			seen := map[*chunkRow]bool{}
			for k, r := range out {
				if seen[r] {
					t.Fatalf("row %d shares its struct with an earlier row", k)
				}
				seen[r] = true
				if r.ID != int64(k+1) {
					t.Fatalf("row %d: id %d", k, r.ID)
				}
				if k%3 == 0 {
					if r.Name != nil {
						t.Fatalf("row %d: name %q, expected NULL", k, *r.Name)
					}
				} else if r.Name == nil || *r.Name != fmt.Sprintf("name-%d", k) {
					t.Fatalf("row %d: name %v", k, r.Name)
				}
			}
			// a write to one row must not show in another
			for k, r := range out {
				r.Pad[0] = byte(k)
			}
			for k, r := range out {
				if r.Pad[0] != byte(k) {
					t.Fatalf("row %d: Pad[0] %d, overwritten through another row", k, r.Pad[0])
				}
			}
		})
	}
}

// TestScanStructSlicePtrFewRowsAllocations: a query that returns one row into
// []*T allocates about one struct, not a chunk of 64. fylr loads the user of
// every request this way; with a chunk of 64 that was 64 users, about 50 KB,
// per request.
func TestScanStructSlicePtrFewRowsAllocations(t *testing.T) {
	size := float64(unsafe.Sizeof(chunkRow{}))
	for _, tc := range []struct {
		rows int
		max  float64 // bytes per scan, in structs
	}{
		{rows: 1, max: 2},
		{rows: 3, max: 6},
		{rows: 10, max: 20},
	} {
		t.Run(fmt.Sprintf("rows=%d", tc.rows), func(t *testing.T) {
			const runs = 50
			wrapper, backend := newFakeSqlPro(t, POSTGRES)
			cols, rows := chunkRows(tc.rows)
			for i := 0; i < runs+1; i++ {
				backend.queueQuery(cols, rows)
			}
			scan := func() {
				var out []*chunkRow
				if err := wrapper.Query(&out, "SELECT id, name FROM t"); err != nil {
					t.Fatal(err)
				}
				if len(out) != tc.rows {
					t.Fatalf("expected %d rows, got %d", tc.rows, len(out))
				}
			}
			scan() // warm the struct info cache

			var before, after runtime.MemStats
			runtime.GC()
			runtime.ReadMemStats(&before)
			for i := 0; i < runs; i++ {
				scan()
			}
			runtime.ReadMemStats(&after)

			perScan := float64(after.TotalAlloc-before.TotalAlloc) / runs
			if perScan > tc.max*size {
				t.Errorf("%d rows: %.0f bytes per scan, %.1f structs of %.0f bytes, expected at most %.0f structs",
					tc.rows, perScan, perScan/size, size, tc.max)
			}
		})
	}
}

// BenchmarkFakeScanPtrRowsFew scans results of a few rows into []*T, the shape
// of most of fylr's loads by id (the user of a request, its groups, a file).
func BenchmarkFakeScanPtrRowsFew(b *testing.B) {
	for _, n := range []int{1, 4, 16} {
		b.Run(fmt.Sprintf("rows=%d", n), func(b *testing.B) {
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
		})
	}
}
