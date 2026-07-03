# sqlpro

`sqlpro` is a small, reflection-based convenience layer over Go's
`database/sql`. It maps Go structs to rows, rewrites placeholders, expands `IN`
clauses, runs transactions with lifecycle hooks, and provides bulk
insert/update helpers — while staying out of your way: you always write the SQL.

It supports **PostgreSQL** (via `pgx`) and **SQLite** (via `modernc.org/sqlite`).

```go
import "github.com/programmfabrik/sqlpro"

db, err := sqlpro.Open("postgres", "host=localhost dbname=app sslmode=disable")
// or: sqlpro.Open("sqlite", "/path/to/app.db?_pragma=journal_mode(WAL)")

type Author struct {
    ID   int64  `db:"id,pk,omitempty"`
    Name string `db:"name"`
}

a := &Author{Name: "Ada"}
db.Insert("author", a)          // INSERT; a.ID is filled in

var authors []*Author
db.Query(&authors, "SELECT * FROM author WHERE name LIKE ?", "A%")
```

## Contents

- [Install](#install)
- [Connecting](#connecting)
- [Mapping structs to rows](#mapping-structs-to-rows)
- [Reading](#reading)
- [Writing](#writing)
- [NULL, JSON and custom column types](#null-json-and-custom-column-types)
- [Placeholders & escaping](#placeholders--escaping)
- [Transactions](#transactions)
- [Introspection](#introspection)
- [Errors](#errors)
- [Examples](#examples)
- [Testing & benchmarks](#testing--benchmarks)

## Install

```sh
go get github.com/programmfabrik/sqlpro
```

Import a driver somewhere in your program (sqlpro selects it by name):

```go
import (
    _ "modernc.org/sqlite"       // for "sqlite"
    // pgx is pulled in by sqlpro itself for "postgres"
)
```

## Connecting

`Open(driver, dsn)` returns a `DB`. `driver` is `"postgres"`, `"sqlite"` or
`"sqlite3"`.

```go
db, err := sqlpro.Open("sqlite", "/tmp/app.db")
defer db.Close()
```

`Open` pings the connection and applies driver-appropriate defaults: PostgreSQL
uses `$1` placeholders and `RETURNING` for generated keys; SQLite uses `?`
placeholders and the RFC3339 time format. Everything below works the same on
both drivers.

`db.DB()` returns the underlying `*sql.DB` if you need something sqlpro does not
wrap.

## Mapping structs to rows

Columns are matched to struct fields by the `db` tag. Only exported fields with
a `db` tag participate. Embedded structs are flattened, so their columns are
promoted as if declared inline.

```go
type Row struct {
    ID   int64  `db:"id,pk,omitempty"`
    Name string `db:"name"`
}
```

Tag options (comma-separated after the column name):

| option | effect |
| --- | --- |
| `pk` | primary key — used as the `WHERE` for `Update`, and written back after `Insert` |
| `omitempty` | skip the column on write when the Go value is zero (lets the DB apply its default / auto-increment) |
| `readonly` | never written by sqlpro (server-generated/computed columns); still read back |
| `json` | the field is JSON-marshaled on write and unmarshaled on read |
| `json_ignore_error` | ignore JSON marshal/unmarshal errors for this field |
| `null` | write SQL `NULL` when the value is zero (for `json` fields: instead of the string `"null"`) |
| `notnull` | for `json` fields: write the literal `"null"` rather than SQL `NULL`; on plain pointer fields a `nil` value panics instead of writing `NULL` |
| `-` | ignore the field entirely (never read or written) |

`db:"name"` with no options just maps the column.

## Reading

`Query` / `QueryContext` pick how to scan from the **type of the target**:

| target | result |
| --- | --- |
| `*int64`, `*string`, `*time.Time`, … | first column of the first row |
| `*struct` | the first row mapped by `db` tags |
| `*[]Struct`, `*[]*Struct` | all rows |
| `*[]int64`, `*[]*string`, … | the first column of all rows |
| `**sql.Rows` | the raw rows handle for manual iteration |

```go
var n int64
db.Query(&n, "SELECT count(*) FROM author")

var a Author
db.Query(&a, "SELECT * FROM author WHERE id = ?", 1)

var all []*Author
db.Query(&all, "SELECT * FROM author ORDER BY name")

var names []string
db.Query(&names, "SELECT name FROM author")
```

A single-row target that matches no row returns
[`ErrQueryReturnedZeroRows`](#errors); a slice target simply comes back empty.

## Writing

```go
db.Insert("author", &a)                  // pk written back into a
db.InsertContext(ctx, "author", &a)

db.Update("author", &a)                  // by pk
db.Save("author", &a)                    // upsert: insert if pk==0, else update

db.Exec("DELETE FROM author WHERE id = ?", 1)
affected, lastID, err := db.ExecContextRowsAffected(ctx, "UPDATE author SET name = ?", "x")
```

Bulk helpers operate on a slice of structs in one round trip: the inserts run
as a single statement, `UpdateBulkContext` sends one `UPDATE` per row in a
single exec. Nothing is chunked: statement-size limits of the database are
the caller's concern.

```go
db.InsertBulk("author", []*Author{{Name: "a"}, {Name: "b"}})     // ids written back
db.InsertBulkOnConflictDoNothingContext(ctx, "author", rows, "name") // skip conflicts on "name"
db.UpdateBulkContext(ctx, "author", rows)                            // update many by pk
```

Like `Insert`, the bulk inserts write the generated primary keys back into
the rows — automatically, whenever the batch qualifies: the struct has
exactly one primary key, a non-pointer signed-integer field tagged
`pk,omitempty` (the column auto-assigned by the database), and no row has its
key pre-set. Such a batch runs as one multi-row `INSERT … RETURNING`; the
returned keys are mapped back by row order, which is correct when the
statement generated all of them. A batch that does not qualify simply inserts
without read-back — on PostgreSQL inside `ExecTX` via the faster `COPY FROM`
(which cannot `RETURNING`; outside `ExecTX` the raw pgx connection needed for
COPY is not available and the literal `INSERT` is used).

With `ON CONFLICT DO NOTHING` the read-back is per row: an inserted row gets
its key, a conflicted (skipped) row keeps its zero key — so a non-zero key
tells you the row was actually inserted. When rows were skipped, the
generated keys are matched back with one follow-up `SELECT` on the conflict
columns (or on all inserted columns when no conflict target was given),
compared by the database itself — so this costs an extra round trip only when
conflicts actually happened. Rows with identical match values are assigned
the same key.

## NULL, JSON and custom column types

- **NULL**: use a pointer field. `nil` ⇄ SQL `NULL`.
- **JSON**: tag the field `db:"col,json"`; it is stored as JSON text.
- **`json.RawMessage`**: stored/loaded verbatim.
- **Custom types**: implement `driver.Valuer` (write) and `sql.Scanner` (read).

```go
type Place struct {
    Name   *string         `db:"name"`        // nullable
    Config Settings        `db:"config,json"` // JSON column
    Raw    json.RawMessage `db:"raw"`         // stored as-is
    At     Geo             `db:"at"`          // Valuer + Scanner
}
```

A custom column type implements both interfaces:

```go
func (g Geo) Value() (driver.Value, error) { return fmt.Sprintf("%g,%g", g.Lat, g.Lng), nil }
func (g *Geo) Scan(v any) error            { /* parse v into *g */ }
```

The helper scanners `NullTime`, `NullJson` and `NullRawMessage` are used
internally and are exported for direct use with `*sql.Rows.Scan`.

## Placeholders & escaping

Write portable `?` placeholders; sqlpro rewrites them to `$1, $2, …` on
PostgreSQL. Special placeholders:

- **`IN ?`** — pass a slice and it expands to the right number of placeholders:

  ```go
  db.Query(&names, "SELECT name FROM author WHERE id IN ?", []int64{1, 2, 3})
  ```

  Slices with more than 100 elements are inlined as escaped literals instead
  of placeholders; that fallback supports only string and int/int32/int64
  element types (plus their pointers).

- **`@`** — the next argument is quoted as a SQL **identifier** (table/column):

  ```go
  db.Query(&n, "SELECT count(*) FROM @", "author")
  ```

Escaping helpers for the rare cases a value can't be a bound argument:

```go
db.EscValue("O'Hara")                 // 'O''Hara'
sqlpro.IlikeSql(db.Driver(), "berg")  // driver-correct case-insensitive LIKE snippet
```

## Transactions

`ExecTX` is the recommended entry point. It opens a dedicated connection, hands
a transaction-carrying `context` to your job, **commits on success** and **rolls
back on error or panic**. Inside the job, get the transaction with `CtxTX(ctx)`
and use it exactly like a `DB`:

```go
err := db.ExecTX(ctx, func(ctx context.Context) error {
    tx := sqlpro.CtxTX(ctx)
    if err := tx.Insert("account", &acc); err != nil {
        return err // -> rollback
    }
    return tx.Exec("UPDATE ledger SET balance = balance - ? WHERE id = ?", amt, id)
}, nil) // *sql.TxOptions, or nil
```

Transactions cannot be nested (`ExecTX` inside `ExecTX` errors). Lifecycle hooks
can be registered on the transaction:

| hook | when |
| --- | --- |
| `BeforeCommit(func() error)` | inside `Commit`, before the underlying commit; an error rolls back |
| `AfterCommit(func())` | after a successful commit |
| `AfterRollback(func())` | after a rollback |
| `AfterTransaction(func())` | after a successful commit **or** rollback |

Use `BeforeCommit` when a side effect must be atomic with the transaction (e.g.
bumping a cache version); use the `After*` hooks for non-transactional effects
(logging, cache invalidation).

For explicit control there are also `Begin()`, `BeginRead()` (read-only) and
`BeginContext()`, each returning a `TX` you `Commit()` / `Rollback()` yourself.

## Introspection

```go
v, _ := db.Version()  // e.g. "Sqlite 3.45.0" / PostgreSQL version string
n, _ := db.Name()     // current database name / sqlite file
db.Log()              // returns a copy with debug logging enabled
```

## Errors

- `ErrQueryReturnedZeroRows` — a single-row `Query` found nothing. Test with
  `errors.Is`.
- `ErrMismatchedRowsAffected` — an operation affected an unexpected number of
  rows.

## Examples

A runnable, end-to-end tour lives in [`examples/`](examples). It exercises every
feature against a throwaway SQLite database:

```sh
go run ./examples
```

| file | covers |
| --- | --- |
| [`crud.go`](examples/crud.go) | Insert / Query / Update / Save / Delete, query target shapes |
| [`bulk_tags.go`](examples/bulk_tags.go) | `InsertBulk`, on-conflict, `UpdateBulk`; `pk`/`omitempty`/`readonly`/`-`/embedding |
| [`null_json.go`](examples/null_json.go) | nullable pointers, JSON columns, `json.RawMessage`, custom `Valuer`/`Scanner`, and the `,null`/`,notnull`/`,json_ignore_error` null-handling options |
| [`placeholders.go`](examples/placeholders.go) | `?`, `IN ?`, `@`, `EscValue`, `IlikeSql` |
| [`transactions.go`](examples/transactions.go) | `ExecTX`, hooks, rollback, `BeginRead`, introspection |

## Testing & benchmarks

Most tests run against SQLite and need no setup. The exception is
`TestCopyFrom`: it needs a local PostgreSQL with an `apitest` database and
fails without one:

```sh
go test ./...                 # SQLite tests + TestCopyFrom (PostgreSQL)
go test -run TestCopyFrom .   # just the PostgreSQL test
```

`feature_test.go` is a from-scratch, feature-by-feature suite covering the full
public surface. The scan path has a benchmark:

```sh
go test -run='^$' -bench=BenchmarkScanRows -benchmem .
```

The slice-of-struct read path is optimized to build its column plan, scan buffer
and null-scanners **once per query** and reuse them across rows, so only the row
struct itself is allocated per row.
