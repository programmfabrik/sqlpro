// Command examples is a runnable tour of the sqlpro API. Every section runs
// against a throwaway SQLite database, so you can read the code and the output
// side by side. It doubles as the worked-example basis for the package README.
//
// Run it with:
//
//	go run ./examples
//
// Each section lives in its own file (crud.go, bulk_tags.go, null_json.go,
// placeholders.go, transactions.go) and is referenced from the README.
package main

import (
	"context"
	"fmt"
	"log"
	"net/url"
	"os"
	"path/filepath"

	_ "modernc.org/sqlite"

	"github.com/programmfabrik/sqlpro"
)

// openExampleDB opens a fresh SQLite database in a temp file. SQLite is used so
// the examples are self-contained; everything shown works the same against
// PostgreSQL (open with sqlpro.Open("postgres", dsn)).
func openExampleDB() (sqlpro.DB, func()) {
	dbFile := filepath.Join(os.TempDir(), "sqlpro_examples.db")
	_ = os.Remove(dbFile)

	// modernc.org/sqlite is configured through DSN query parameters.
	qv := url.Values{}
	qv.Add("_pragma", "foreign_keys(1)")
	qv.Add("_pragma", "busy_timeout(10000)")
	qv.Add("_pragma", "journal_mode(WAL)")
	qv.Add("_time_format", "sqlite")

	db, err := sqlpro.Open("sqlite", dbFile+"?"+qv.Encode())
	if err != nil {
		log.Fatalf("open: %v", err)
	}
	return db, func() {
		db.Close()
		_ = os.Remove(dbFile)
	}
}

func main() {
	db, cleanup := openExampleDB()
	defer cleanup()

	ctx := context.Background()

	sections := []struct {
		name string
		fn   func(context.Context, sqlpro.DB) error
	}{
		{"crud", crudExample},
		{"query forms", queryExample},
		{"bulk", bulkExample},
		{"struct tags", tagsExample},
		{"null & json", nullJSONExample},
		{"null annotations", nullAnnotationsExample},
		{"placeholders & escaping", placeholderExample},
		{"transactions", transactionExample},
		{"introspection", introspectionExample},
	}

	for _, s := range sections {
		fmt.Printf("\n==== %s ====\n", s.name)
		if err := s.fn(ctx, db); err != nil {
			log.Fatalf("%s: %v", s.name, err)
		}
	}
	fmt.Println("\nall examples completed ✔")
}
