package sqlpro

import (
	"database/sql"
	"testing"
)

// newBenchFakeDB returns a sqlpro wrapper connected to the in-package fake
// driver (fakedb_test.go), SQLITE3-mode, without needing a *testing.T.
func newBenchFakeDB() (*db, *fakeBackend) {
	backend := &fakeBackend{}
	conn := sql.OpenDB(&fakeConnector{backend: backend})
	wrapper := newSqlPro(conn)
	wrapper.sqlDB = conn
	wrapper.driver = SQLITE3
	return wrapper, backend
}

// benchExecRows builds n rows of the 16-column benchScanRow with the pk set
// non-zero, so every Insert/Update sees the identical 16-column value set.
func benchExecRows(n int) []*benchScanRow {
	rows := make([]*benchScanRow, 0, n)
	for k := 0; k < n; k++ {
		rows = append(rows, &benchScanRow{
			A: int64(k + 1),
			B: "beta", C: "gamma", D: int64(k), E: int64(k * 2), F: "foxtrot",
			G: "golf", H: int64(k), I: int64(k), J: "juliet", K: "kilo",
			L: int64(k), M: "mike", N: int64(k), O: "oscar", P: "papa",
		})
	}
	return rows
}

// BenchmarkInsertClauseFromValues isolates the per-row INSERT SQL build
// (exec.go insertClauseFromValues), the function InsertContext calls once per
// row of a slice.
func BenchmarkInsertClauseFromValues(b *testing.B) {
	dbh, _ := newBenchFakeDB()
	row := *benchExecRows(1)[0]
	values, info, err := dbh.valuesFromStruct(row)
	if err != nil {
		b.Fatal(err)
	}
	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		if _, _, err := dbh.insertClauseFromValues("bench", values, info); err != nil {
			b.Fatal(err)
		}
	}
}

// BenchmarkUpdateClauseFromRow isolates the per-row UPDATE SQL build
// (exec.go updateClauseFromRow), incl. its internal valuesFromStruct.
func BenchmarkUpdateClauseFromRow(b *testing.B) {
	dbh, _ := newBenchFakeDB()
	row := *benchExecRows(1)[0]
	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		if _, _, err := dbh.updateClauseFromRow("bench", row); err != nil {
			b.Fatal(err)
		}
	}
}

// BenchmarkInsertSlice100Fake measures InsertContext over a slice of 100
// structs end-to-end through the fake driver: per-row valuesFromStruct +
// insertClauseFromValues + replaceArgs + database/sql Exec.
func BenchmarkInsertSlice100Fake(b *testing.B) {
	dbh, backend := newBenchFakeDB()
	rows := benchExecRows(100)
	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		if err := dbh.Insert("bench", rows); err != nil {
			b.Fatal(err)
		}
		backend.mu.Lock()
		backend.statements = nil
		backend.mu.Unlock()
	}
}

// BenchmarkUpdateSlice100Fake measures UpdateContext over a slice of 100
// structs end-to-end through the fake driver.
func BenchmarkUpdateSlice100Fake(b *testing.B) {
	dbh, backend := newBenchFakeDB()
	rows := benchExecRows(100)
	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		if err := dbh.Update("bench", rows); err != nil {
			b.Fatal(err)
		}
		backend.mu.Lock()
		backend.statements = nil
		backend.mu.Unlock()
	}
}

// TestInsertSQLDistinct reports how many distinct SQL texts 300 single-row
// INSERT builds of the same struct produce. Every distinct text is a separate
// pgx prepared-statement cache entry (QueryExecModeCacheStatement keys on the
// exact SQL string).
func TestInsertSQLDistinct(t *testing.T) {
	dbh, _ := newBenchFakeDB()
	row := *benchExecRows(1)[0]
	values, info, err := dbh.valuesFromStruct(row)
	if err != nil {
		t.Fatal(err)
	}
	distinct := map[string]bool{}
	for i := 0; i < 300; i++ {
		s, _, err := dbh.insertClauseFromValues("bench", values, info)
		if err != nil {
			t.Fatal(err)
		}
		distinct[s] = true
	}
	t.Logf("distinct INSERT SQL strings over 300 builds of one struct: %d", len(distinct))
}
