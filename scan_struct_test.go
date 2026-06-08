package sqlpro

import (
	"encoding/json"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

// TestScanStructSlicePrepareOnce validates the prepare-once slice-of-struct scan
// path (reused scanners/buffer): distinct values per row catch cross-row
// aliasing, interleaved NULLs catch the scanner-reset handling, across value
// and pointer fields plus time, json and json.RawMessage.
func TestScanStructSlicePrepareOnce(t *testing.T) {
	type prepRow struct {
		A  int64           `db:"a,pk,omitempty"`
		S  string          `db:"s"`  // value string
		SP *string         `db:"sp"` // pointer string, sometimes NULL
		N  int64           `db:"n"`  // value int
		NP *int64          `db:"np"` // pointer int, sometimes NULL
		F  float64         `db:"f"`
		B  bool            `db:"b"`
		T  *time.Time      `db:"t"` // pointer time, sometimes NULL
		J  myStruct        `db:"j,json"`
		R  json.RawMessage `db:"r"`
	}

	if err := dbConn.Exec(`DROP TABLE IF EXISTS prep_test`); err != nil {
		t.Fatal(err)
	}
	err := dbConn.Exec(`CREATE TABLE prep_test(
		a INTEGER PRIMARY KEY AUTOINCREMENT,
		s TEXT, sp TEXT, n INTEGER, np INTEGER, f REAL, b INTEGER, t DATETIME, j TEXT, r TEXT)`)
	if err != nil {
		t.Fatal(err)
	}

	now := time.Now()
	const N = 60
	ins := make([]*prepRow, 0, N)
	for k := 0; k < N; k++ {
		r := &prepRow{
			S: fmt.Sprintf("s-%d", k),
			N: int64(k),
			F: float64(k) + 0.5,
			B: k%2 == 0,
			J: myStruct{A: fmt.Sprintf("a-%d", k), B: "x"},
			R: json.RawMessage(fmt.Sprintf(`{"k":%d}`, k)),
		}
		if k%3 != 0 { // leave SP/NP NULL every third row
			sp := fmt.Sprintf("sp-%d", k)
			np := int64(k * 10)
			r.SP, r.NP = &sp, &np
		}
		if k%4 != 0 { // leave T NULL every fourth row
			tv := now.Add(time.Duration(k) * time.Second)
			r.T = &tv
		}
		ins = append(ins, r)
	}
	if err := dbConn.InsertBulk("prep_test", ins); err != nil {
		t.Fatal(err)
	}

	var out []*prepRow
	if err := dbConn.Query(&out, "SELECT * FROM prep_test ORDER BY a"); err != nil {
		t.Fatal(err)
	}
	if !assert.Len(t, out, N) {
		return
	}

	for k, r := range out {
		assert.Equalf(t, fmt.Sprintf("s-%d", k), r.S, "row %d S", k)
		assert.Equalf(t, int64(k), r.N, "row %d N", k)
		assert.Equalf(t, float64(k)+0.5, r.F, "row %d F", k)
		assert.Equalf(t, k%2 == 0, r.B, "row %d B", k)
		assert.Equalf(t, fmt.Sprintf("a-%d", k), r.J.A, "row %d J.A", k)
		assert.Equalf(t, fmt.Sprintf(`{"k":%d}`, k), string(r.R), "row %d R", k)

		if k%3 != 0 {
			if assert.NotNilf(t, r.SP, "row %d SP", k) {
				assert.Equalf(t, fmt.Sprintf("sp-%d", k), *r.SP, "row %d *SP", k)
			}
			if assert.NotNilf(t, r.NP, "row %d NP", k) {
				assert.Equalf(t, int64(k*10), *r.NP, "row %d *NP", k)
			}
		} else {
			assert.Nilf(t, r.SP, "row %d SP should be NULL", k)
			assert.Nilf(t, r.NP, "row %d NP should be NULL", k)
		}

		if k%4 != 0 {
			assert.NotNilf(t, r.T, "row %d T", k)
		} else {
			assert.Nilf(t, r.T, "row %d T should be NULL", k)
		}
	}

	// Also exercise the value-slice ([]struct) variant.
	var outVal []prepRow
	if err := dbConn.Query(&outVal, "SELECT * FROM prep_test ORDER BY a"); err != nil {
		t.Fatal(err)
	}
	if assert.Len(t, outVal, N) {
		for k, r := range outVal {
			assert.Equalf(t, int64(k), r.N, "value-slice row %d N", k)
			assert.Equalf(t, fmt.Sprintf("s-%d", k), r.S, "value-slice row %d S", k)
		}
	}

	_ = dbConn.Exec(`DROP TABLE IF EXISTS prep_test`)
}
