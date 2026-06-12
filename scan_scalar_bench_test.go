package sqlpro

import "testing"

// benchScalarSetup creates a table with n rows for the scalar-slice scan
// benchmarks ([]int64 / []string targets — the Scan() slice mode for
// non-struct elements).
func benchScalarSetup(b *testing.B, n int) {
	if err := dbConn.Exec(`DROP TABLE IF EXISTS bench_scalar`); err != nil {
		b.Fatal(err)
	}
	err := dbConn.Exec(`CREATE TABLE bench_scalar(
		id INTEGER PRIMARY KEY AUTOINCREMENT,
		name TEXT)`)
	if err != nil {
		b.Fatal(err)
	}
	type row struct {
		ID   int64  `db:"id,pk,omitempty"`
		Name string `db:"name"`
	}
	rows := make([]*row, 0, n)
	for k := 0; k < n; k++ {
		rows = append(rows, &row{Name: "some-name"})
	}
	if err := dbConn.InsertBulk("bench_scalar", rows); err != nil {
		b.Fatal(err)
	}
}

// BenchmarkScanScalarInt64 measures Query + Scan of n int64 IDs into []int64
// through real SQLite rows — e.g. the "fetch all matching IDs" pattern.
func BenchmarkScanScalarInt64(b *testing.B) {
	const n = 5000
	benchScalarSetup(b, n)

	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		var out []int64
		if err := dbConn.Query(&out, "SELECT id FROM bench_scalar"); err != nil {
			b.Fatal(err)
		}
		if len(out) != n {
			b.Fatalf("expected %d rows, got %d", n, len(out))
		}
	}
}

// BenchmarkScanScalarString measures Query + Scan of n strings into []string.
func BenchmarkScanScalarString(b *testing.B) {
	const n = 5000
	benchScalarSetup(b, n)

	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		var out []string
		if err := dbConn.Query(&out, "SELECT name FROM bench_scalar"); err != nil {
			b.Fatal(err)
		}
		if len(out) != n {
			b.Fatalf("expected %d rows, got %d", n, len(out))
		}
	}
}
