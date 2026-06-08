package sqlpro

import "testing"

// benchScanRow is a moderately wide row (16 db-mapped columns), representative
// of the structs read in bulk by callers like fylr.
type benchScanRow struct {
	A int64  `db:"a,pk,omitempty"`
	B string `db:"b"`
	C string `db:"c"`
	D int64  `db:"d"`
	E int64  `db:"e"`
	F string `db:"f"`
	G string `db:"g"`
	H int64  `db:"h"`
	I int64  `db:"i"`
	J string `db:"j"`
	K string `db:"k"`
	L int64  `db:"l"`
	M string `db:"m"`
	N int64  `db:"n"`
	O string `db:"o"`
	P string `db:"p"`
}

func benchScanSetup(b *testing.B, n int) {
	if err := dbConn.Exec(`DROP TABLE IF EXISTS bench_scan`); err != nil {
		b.Fatal(err)
	}
	err := dbConn.Exec(`CREATE TABLE bench_scan(
		a INTEGER PRIMARY KEY AUTOINCREMENT,
		b TEXT, c TEXT, d INTEGER, e INTEGER, f TEXT, g TEXT, h INTEGER,
		i INTEGER, j TEXT, k TEXT, l INTEGER, m TEXT, n INTEGER, o TEXT, p TEXT)`)
	if err != nil {
		b.Fatal(err)
	}
	rows := make([]*benchScanRow, 0, n)
	for k := 0; k < n; k++ {
		rows = append(rows, &benchScanRow{
			B: "beta", C: "gamma", D: int64(k), E: int64(k * 2), F: "foxtrot",
			G: "golf", H: int64(k), I: int64(k), J: "juliet", K: "kilo",
			L: int64(k), M: "mike", N: int64(k), O: "oscar", P: "papa",
		})
	}
	if err := dbConn.InsertBulk("bench_scan", rows); err != nil {
		b.Fatal(err)
	}
}

// BenchmarkScanRows measures a full Query + Scan of n rows into []*struct
// through real SQLite rows — the end-to-end struct-reading path. Run:
//
//	go test -run='^$' -bench=BenchmarkScanRows -benchmem .
func BenchmarkScanRows(b *testing.B) {
	const n = 2000
	benchScanSetup(b, n)

	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		var out []*benchScanRow
		if err := dbConn.Query(&out, "SELECT * FROM bench_scan"); err != nil {
			b.Fatal(err)
		}
		if len(out) != n {
			b.Fatalf("expected %d rows, got %d", n, len(out))
		}
	}
}
