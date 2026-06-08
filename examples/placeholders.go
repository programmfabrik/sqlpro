package main

import (
	"context"
	"fmt"

	"github.com/programmfabrik/sqlpro"
)

// placeholderExample shows sqlpro's query-building helpers: the "?" argument
// placeholder (rewritten to $1.. on PostgreSQL automatically), slice expansion
// for "IN ?", the "@" identifier placeholder, and the Esc/EscValue/IlikeSql
// escaping helpers.
func placeholderExample(ctx context.Context, db sqlpro.DB) error {
	err := db.Exec(`CREATE TABLE product(
		id    INTEGER PRIMARY KEY AUTOINCREMENT,
		name  TEXT NOT NULL,
		price INTEGER)`)
	if err != nil {
		return err
	}

	type product struct {
		ID    int64  `db:"id,pk,omitempty"`
		Name  string `db:"name"`
		Price int64  `db:"price,omitempty"`
	}
	seed := []*product{
		{Name: "Apple"}, {Name: "Banana"}, {Name: "Cherry"}, {Name: "Date"},
	}
	if err := db.InsertBulk("product", seed); err != nil {
		return err
	}

	// "?" is the portable placeholder. Pass a slice to "IN ?" and sqlpro expands
	// it to the right number of placeholders.
	var names []string
	err = db.Query(&names,
		"SELECT name FROM product WHERE name IN ? ORDER BY name",
		[]string{"Apple", "Cherry", "Elderberry"})
	if err != nil {
		return err
	}
	fmt.Printf("IN ? matched: %v\n", names)

	// "@" is the identifier placeholder: the argument is quoted as a SQL name
	// (so "product" becomes "product"). Useful for dynamic table/column names.
	var count int64
	if err := db.Query(&count, "SELECT count(*) FROM @", "product"); err != nil {
		return err
	}
	fmt.Printf("rows via @ table placeholder: %d\n", count)

	// EscValue quotes a string literal for the rare cases where a value cannot
	// be a bound argument. (Identifiers are best quoted with the "@" placeholder
	// shown above.)
	fmt.Printf("EscValue(%q)=%s\n", `O'Hara`, db.EscValue(`O'Hara`))

	// IlikeSql builds a driver-correct case-insensitive LIKE snippet.
	var like []string
	if err := db.Query(&like,
		"SELECT name FROM product WHERE name "+sqlpro.IlikeSql(db.Driver(), "an")); err != nil {
		return err
	}
	fmt.Printf("case-insensitive match for 'an': %v\n", like)
	return nil
}
