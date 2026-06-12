package sqlpro

import (
	"database/sql"
	"fmt"
	"testing"
	"time"
)

// benchBulkRow mirrors a typical fylr bulk-insert row: integer pk (omitempty,
// auto-assigned) plus 6 data columns, half strings, half numbers.
type benchBulkRow struct {
	A int64   `db:"a,pk,omitempty"`
	B string  `db:"b"`
	C string  `db:"c"`
	D int64   `db:"d"`
	E float64 `db:"e"`
	F string  `db:"f"`
	G int64   `db:"g"`
}

func benchBulkRows(n int) []*benchBulkRow {
	rows := make([]*benchBulkRow, 0, n)
	for i := 0; i < n; i++ {
		rows = append(rows, &benchBulkRow{
			B: fmt.Sprintf("object-%d", i),
			C: "some text value with a 'quote' in it",
			D: int64(i),
			E: float64(i) * 1.5,
			F: "kilo",
			G: int64(i * 7),
		})
	}
	return rows
}

// BenchmarkInsertBulkPrepFake measures InsertBulk of 5000 rows x 6 columns
// against the no-op fake driver, isolating the sqlpro-side preparation work
// (row materialization + SQL literal building) from real database cost.
func BenchmarkInsertBulkPrepFake(b *testing.B) {
	backend := &fakeBackend{}
	conn := sql.OpenDB(&fakeConnector{backend: backend})
	defer conn.Close()

	wrapper := newSqlPro(conn)
	wrapper.sqlDB = conn
	wrapper.driver = SQLITE3
	wrapper.timeFormat = time.RFC3339Nano

	const n = 5000
	rows := benchBulkRows(n)

	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		backend.queueExec(int64(n), 0)
		if err := wrapper.InsertBulk("bench_bulk", rows); err != nil {
			b.Fatal(err)
		}
		// drop the recorded statement so memory does not grow with b.N
		backend.mu.Lock()
		backend.statements = backend.statements[:0]
		backend.mu.Unlock()
	}
}

// BenchmarkInsertBulkSQLite measures the same InsertBulk end-to-end through
// real SQLite (the non-postgres production path).
func BenchmarkInsertBulkSQLite(b *testing.B) {
	if err := dbConn.Exec(`DROP TABLE IF EXISTS bench_bulk`); err != nil {
		b.Fatal(err)
	}
	err := dbConn.Exec(`CREATE TABLE bench_bulk(
		a INTEGER PRIMARY KEY AUTOINCREMENT,
		b TEXT, c TEXT, d INTEGER, e REAL, f TEXT, g INTEGER)`)
	if err != nil {
		b.Fatal(err)
	}

	const n = 5000
	rows := benchBulkRows(n)

	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		if err := dbConn.InsertBulk("bench_bulk", rows); err != nil {
			b.Fatal(err)
		}
		b.StopTimer()
		if err := dbConn.Exec("DELETE FROM bench_bulk"); err != nil {
			b.Fatal(err)
		}
		b.StartTimer()
	}
}
