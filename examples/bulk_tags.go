package main

import (
	"context"
	"fmt"

	"github.com/programmfabrik/sqlpro"
)

type city struct {
	ID   int64  `db:"id,pk,omitempty"`
	Name string `db:"name"`
	Pop  int64  `db:"pop"`
}

// bulkExample shows the set-at-a-time write helpers. On PostgreSQL InsertBulk
// transparently uses COPY FROM for large batches; on SQLite it builds multi-row
// INSERTs (chunked to stay under the placeholder limit).
func bulkExample(ctx context.Context, db sqlpro.DB) error {
	err := db.Exec(`CREATE TABLE city(
		id   INTEGER PRIMARY KEY AUTOINCREMENT,
		name TEXT NOT NULL UNIQUE,
		pop  INTEGER)`)
	if err != nil {
		return err
	}

	rows := []*city{
		{Name: "Berlin", Pop: 3700000},
		{Name: "Hamburg", Pop: 1900000},
		{Name: "Munich", Pop: 1500000},
	}

	// One round-trip for all rows.
	if err := db.InsertBulk("city", rows); err != nil {
		return err
	}
	fmt.Printf("bulk-inserted %d cities\n", len(rows))

	// Skip rows that would violate a unique/primary key instead of erroring.
	dupes := []*city{
		{Name: "Berlin", Pop: 9999999}, // already exists -> skipped
		{Name: "Cologne", Pop: 1100000},
	}
	if err := db.InsertBulkOnConflictDoNothingContext(ctx, "city", dupes, "name"); err != nil {
		return err
	}

	// UpdateBulk updates many rows by primary key in one call. (InsertBulk does
	// not read generated keys back, so load the rows first to get their ids.)
	var all []*city
	if err := db.Query(&all, "SELECT * FROM city"); err != nil {
		return err
	}
	for _, r := range all {
		r.Pop++
	}
	if err := db.UpdateBulkContext(ctx, "city", all); err != nil {
		return err
	}

	var total int64
	if err := db.Query(&total, "SELECT count(*) FROM city"); err != nil {
		return err
	}
	fmt.Printf("cities in table: %d\n", total)
	return nil
}

// tagged demonstrates the db-struct tags. Embedded structs are flattened, so
// "meta" columns map as if declared inline.
type tagged struct {
	audit // embedded -> its columns are promoted

	ID    int64  `db:"id,pk,omitempty"` // primary key, omitted when zero
	Name  string `db:"name"`
	Notes string `db:"-"`             // "-" => never read or written
	Slug  string `db:"slug,readonly"` // read back, never written by sqlpro
}

type audit struct {
	Source string `db:"source,omitempty"`
}

// tagsExample shows omitempty, readonly, "-" and embedding in one round-trip.
func tagsExample(ctx context.Context, db sqlpro.DB) error {
	// "slug" has a DB default and is generated server-side; sqlpro never writes
	// it (readonly) but reads it back.
	err := db.Exec(`CREATE TABLE doc(
		id     INTEGER PRIMARY KEY AUTOINCREMENT,
		name   TEXT NOT NULL,
		source TEXT NOT NULL DEFAULT 'manual',
		slug   TEXT NOT NULL DEFAULT 'auto-slug')`)
	if err != nil {
		return err
	}

	t := &tagged{
		Name:  "Spec",
		Notes: "this stays in Go only", // db:"-" -> not persisted
		Slug:  "ignored-on-write",      // readonly -> not persisted
	}
	if err := db.Insert("doc", t); err != nil {
		return err
	}

	var back tagged
	if err := db.Query(&back, "SELECT * FROM doc WHERE id = ?", t.ID); err != nil {
		return err
	}
	// source defaulted ('manual'), slug filled by the DB default, Notes empty.
	fmt.Printf("source=%q slug=%q notes=%q\n", back.Source, back.Slug, back.Notes)
	return nil
}
