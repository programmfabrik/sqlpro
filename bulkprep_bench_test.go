package sqlpro

import "testing"

// benchInsertRow mirrors the finding's shape: 6 simple fields
// (bulk insert during fylr indexing).
type benchInsertRow struct {
	ID   int64   `db:"id,pk,omitempty"`
	Name string  `db:"name"`
	Kind string  `db:"kind"`
	Num  int64   `db:"num"`
	Val  float64 `db:"val"`
	Note string  `db:"note,omitempty"`
}

func makeBenchInsertRows(n int) []*benchInsertRow {
	rows := make([]*benchInsertRow, 0, n)
	for i := 0; i < n; i++ {
		rows = append(rows, &benchInsertRow{
			Name: "name-a", Kind: "kind-b", Num: int64(i), Val: float64(i) * 1.5,
		})
	}
	return rows
}

// BenchmarkBulkRowPrep measures the Go-side row preparation done by
// insertBulkContext: valuesFromStruct per row plus escValueForInsert per
// emitted value. This is exactly the path where isZero runs twice per field.
func BenchmarkBulkRowPrep(b *testing.B) {
	const n = 1000
	rows := makeBenchInsertRows(n)
	db2 := dbConn.(*db)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		for _, r := range rows {
			values, info, err := db2.valuesFromStruct(*r)
			if err != nil {
				b.Fatal(err)
			}
			for key, value := range values {
				_ = db2.escValueForInsert(value, info[key])
			}
		}
	}
}

// BenchmarkInsertBulkSQLite measures the full InsertBulk path (prep + exec)
// through the real SQLite connection from TestMain.
