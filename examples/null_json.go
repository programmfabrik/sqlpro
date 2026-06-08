package main

import (
	"context"
	"database/sql/driver"
	"encoding/json"
	"fmt"
	"time"

	"github.com/programmfabrik/sqlpro"
)

// geo is a custom column type: it implements driver.Valuer (write) and
// sql.Scanner (read), so sqlpro stores/loads it through those methods. Any type
// that does this works as a column value.
type geo struct {
	Lat, Lng float64
}

func (g geo) Value() (driver.Value, error) { return fmt.Sprintf("%g,%g", g.Lat, g.Lng), nil }

func (g *geo) Scan(v any) error {
	if v == nil {
		*g = geo{}
		return nil
	}
	_, err := fmt.Sscanf(fmt.Sprintf("%s", v), "%g,%g", &g.Lat, &g.Lng)
	return err
}

// settings is stored as JSON in a single column via the ",json" tag option.
type settings struct {
	Theme string `json:"theme"`
	Beta  bool   `json:"beta"`
}

type place struct {
	ID int64 `db:"id,pk,omitempty"`
	// Pointers map to nullable columns: nil <-> SQL NULL.
	Name    *string         `db:"name"`
	Founded *time.Time      `db:"founded"`
	At      geo             `db:"at"`          // custom Valuer/Scanner
	Config  settings        `db:"config,json"` // marshaled to/from JSON
	Raw     json.RawMessage `db:"raw"`         // stored verbatim
}

// nullJSONExample shows nullable pointers, JSON columns, json.RawMessage and a
// custom column type round-tripping through the database.
func nullJSONExample(ctx context.Context, db sqlpro.DB) error {
	err := db.Exec(`CREATE TABLE place(
		id      INTEGER PRIMARY KEY AUTOINCREMENT,
		name    TEXT,
		founded DATETIME,
		at      TEXT,
		config  TEXT,
		raw     TEXT)`)
	if err != nil {
		return err
	}

	name := "Lighthouse"
	founded := time.Date(1822, 7, 1, 0, 0, 0, 0, time.UTC)
	full := &place{
		Name:    &name,
		Founded: &founded,
		At:      geo{Lat: 53.5, Lng: 8.1},
		Config:  settings{Theme: "dark", Beta: true},
		Raw:     json.RawMessage(`{"tags":["a","b"]}`),
	}
	if err := db.Insert("place", full); err != nil {
		return err
	}

	// A row with NULLs: leave the pointer fields nil.
	if err := db.Insert("place", &place{Config: settings{Theme: "light"}}); err != nil {
		return err
	}

	var places []*place
	if err := db.Query(&places, "SELECT * FROM place ORDER BY id"); err != nil {
		return err
	}
	for _, p := range places {
		nameStr := "<nil>"
		if p.Name != nil {
			nameStr = *p.Name
		}
		fmt.Printf("place %d: name=%s founded?%v at=%v theme=%s raw=%s\n",
			p.ID, nameStr, p.Founded != nil, p.At, p.Config.Theme, p.Raw)
	}
	return nil
}

// nullAnnotationsExample shows the column-level null-handling tag options:
// ",null", ",notnull" and ",json_ignore_error".
func nullAnnotationsExample(ctx context.Context, db sqlpro.DB) error {
	err := db.Exec(`CREATE TABLE annot(
		id       INTEGER PRIMARY KEY AUTOINCREMENT,
		nullable INTEGER,
		jdef     TEXT,
		jnull    TEXT,
		jnotnull TEXT)`)
	if err != nil {
		return err
	}

	type cfg struct {
		A int `json:"a"`
	}
	type row struct {
		ID int64 `db:"id,pk,omitempty"`
		// ",null" on a value field: a zero value is stored as SQL NULL.
		// (Without it a zero int would be stored as 0.)
		Nullable int64 `db:"nullable,null"`
		// JSON null handling for a zero value (a nil pointer marshals to "null"):
		JDef     *cfg `db:"jdef,json"`             // default   -> SQL NULL
		JNull    *cfg `db:"jnull,json,null"`       // ",null"   -> SQL NULL
		JNotNull *cfg `db:"jnotnull,json,notnull"` // ",notnull"-> literal text "null"
	}

	// Insert a row with everything left zero/nil.
	if err := db.Insert("annot", &row{}); err != nil {
		return err
	}

	var nullableNull, jdefNull, jnullNull, jnotnullNull bool
	for tgt, q := range map[*bool]string{
		&nullableNull: "SELECT nullable IS NULL FROM annot",
		&jdefNull:     "SELECT jdef IS NULL FROM annot",
		&jnullNull:    "SELECT jnull IS NULL FROM annot",
		&jnotnullNull: "SELECT jnotnull IS NULL FROM annot",
	} {
		if err := db.Query(tgt, q); err != nil {
			return err
		}
	}
	fmt.Printf("value ,null -> NULL: %v\n", nullableNull)
	fmt.Printf("json default -> NULL: %v | json ,null -> NULL: %v | json ,notnull -> NULL: %v\n",
		jdefNull, jnullNull, jnotnullNull)

	var rawNotNull string
	if err := db.Query(&rawNotNull, "SELECT jnotnull FROM annot"); err != nil {
		return err
	}
	fmt.Printf("json ,notnull stored literally as: %q\n", rawNotNull)

	// Note: ",notnull" on a *pointer* field rejects a nil value by panicking,
	// which prevents an accidental NULL (not demonstrated here).

	// ",json_ignore_error": invalid JSON in the column reads back as the zero
	// value instead of failing the query.
	if err := db.Exec(`INSERT INTO annot(jdef) VALUES ('not valid json')`); err != nil {
		return err
	}
	type lenient struct {
		J cfg `db:"jdef,json,json_ignore_error"`
	}
	var l lenient
	if err := db.Query(&l, "SELECT jdef FROM annot WHERE jdef = 'not valid json'"); err != nil {
		return err
	}
	fmt.Printf("invalid json with ,json_ignore_error read as zero value: %+v\n", l.J)
	return nil
}
