package main

import (
	"context"
	"errors"
	"fmt"

	"github.com/programmfabrik/sqlpro"
)

// author maps to the "author" table. The db tag names the column; the
// "pk,omitempty" options mark the primary key and tell sqlpro to leave the
// column out of INSERTs when it is zero (so the DB auto-assigns it and writes
// the new id back into the struct).
type author struct {
	ID   int64  `db:"id,pk,omitempty"`
	Name string `db:"name"`
	Born int64  `db:"born"`
}

// crudExample shows the single-row create / read / update / upsert / delete
// cycle.
func crudExample(ctx context.Context, db sqlpro.DB) error {
	err := db.Exec(`CREATE TABLE author(
		id   INTEGER PRIMARY KEY AUTOINCREMENT,
		name TEXT NOT NULL,
		born INTEGER)`)
	if err != nil {
		return err
	}

	// Insert. Because ID is zero and tagged omitempty, it is not sent; SQLite
	// assigns it and sqlpro writes it back into a.ID.
	a := &author{Name: "Ada Lovelace", Born: 1815}
	if err := db.Insert("author", a); err != nil {
		return err
	}
	fmt.Printf("inserted author id=%d\n", a.ID)

	// Read one row into a struct.
	var got author
	if err := db.Query(&got, "SELECT * FROM author WHERE id = ?", a.ID); err != nil {
		return err
	}
	fmt.Printf("loaded: %s (%d)\n", got.Name, got.Born)

	// Update by primary key.
	got.Born = 1816
	if err := db.Update("author", &got); err != nil {
		return err
	}

	// Save = upsert: insert when the pk is zero, update when it is set.
	if err := db.Save("author", &author{Name: "Grace Hopper", Born: 1906}); err != nil {
		return err
	}
	if err := db.Save("author", &got); err != nil { // got has a pk -> update
		return err
	}

	// Delete with a plain Exec.
	if err := db.Exec("DELETE FROM author WHERE name = ?", "Grace Hopper"); err != nil {
		return err
	}

	var count int64
	if err := db.Query(&count, "SELECT count(*) FROM author"); err != nil {
		return err
	}
	fmt.Printf("authors remaining: %d\n", count)
	return nil
}

// queryExample shows the shapes Query can scan into, chosen automatically from
// the target's type.
func queryExample(ctx context.Context, db sqlpro.DB) error {
	// Scalar: first column of the first row.
	var n int64
	if err := db.Query(&n, "SELECT count(*) FROM author"); err != nil {
		return err
	}
	fmt.Printf("scalar count: %d\n", n)

	// Slice of struct pointers: all rows, all mapped columns.
	var authors []*author
	if err := db.Query(&authors, "SELECT * FROM author ORDER BY id"); err != nil {
		return err
	}
	fmt.Printf("loaded %d authors into []*author\n", len(authors))

	// Slice of a single column.
	var names []string
	if err := db.Query(&names, "SELECT name FROM author ORDER BY name"); err != nil {
		return err
	}
	fmt.Printf("names: %v\n", names)

	// A query that finds nothing returns ErrQueryReturnedZeroRows for single-row
	// targets (slices simply come back empty).
	var missing author
	err := db.Query(&missing, "SELECT * FROM author WHERE id = ?", -1)
	if errors.Is(err, sqlpro.ErrQueryReturnedZeroRows) {
		fmt.Println("no row found -> ErrQueryReturnedZeroRows (as expected)")
	} else if err != nil {
		return err
	}

	// Need the raw *sql.Rows? Pass **sql.Rows and iterate yourself.
	// (left out here to keep the example output short)
	return nil
}
