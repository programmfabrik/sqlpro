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
db.InsertBulk("author", []*Author{{Name: "a"}, {Name: "b"}})     // fast, ids NOT read back
db.InsertBulkReadbackIdsContext(ctx, "author", rows)             // ids written back into rows
db.InsertBulkOnConflictDoNothingContext(ctx, "author", rows, "name") // skip conflicts on "name"
db.UpdateBulkContext(ctx, "author", rows)                            // update many by pk
```

By default the bulk inserts do **not** read generated primary keys back — on
PostgreSQL inside `ExecTX` they use the faster `COPY FROM` (outside `ExecTX`
the raw pgx connection COPY needs is unavailable, so a literal multi-row
`INSERT` is used). Reading the keys back would forgo COPY, so it is opt-in.

`InsertBulkReadbackIdsContext` writes the generated keys back into the rows,
like `Insert` does for a single row: it runs one multi-row
`INSERT … RETURNING` and maps the returned keys back by row order. That
requires a single settable non-pointer signed-integer primary key tagged
`pk,omitempty` (auto-assigned by the database), a homogeneous slice, and no
row with its key pre-set — each is checked and returns an error otherwise.
The client-side scan of the returned ids is cheap; the only cost over a plain
`InsertBulk` is losing COPY, which only matters for large batches.

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

Transactions cannot be nested (`ExecTX` inside `ExecTX` errors) — except on an
explicitly adopted leased transaction, see below. Lifecycle hooks can be
registered on the transaction:

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

### Leasing a transaction to another goroutine

An open **write** transaction can be handed to another goroutine — e.g. a
re-entrant HTTP request made by a worker the owning request started and now
waits for. `Lease()` registers the TX under a crypto-random id (a capability:
whoever presents it joins the transaction); `AdoptTX` resolves it into a fresh
context:

```go
// owner side — inside ExecTX, parked while the id is out:
tx := sqlpro.CtxTX(ctx)
id, stop := tx.Lease()
defer stop()                       // Commit/Rollback also invalidate the lease
handToWorkerAndWait(id)            // owner MUST NOT use or end the TX meanwhile

// adopter side — a different goroutine, its own context:
ctx, release, err := sqlpro.AdoptTX(reqCtx, id)
if err != nil { ... }              // unknown, stopped, ended, or failed lease
defer release()                    // serializes adopters: next AdoptTX blocks until released
```

The adopted context carries the owner's TX: plain `CtxTX(ctx)` reads and writes
see the owner's uncommitted state. An `ExecTX` on an adopted context does NOT
error — it runs the job directly on the owner's transaction. **The leased TX
fails as a unit on adopter error**: when a write-intent join (`opts` nil or not
`ReadOnly`) returns an error or panics, the transaction is marked failed — the
owner's `Commit` refuses, rolls back, and returns the adopter's error, so a
partially applied adopter job can never become durable. A `ReadOnly` join's
error is only returned and leaves the transaction healthy (probing reads must
not doom the owner). Commit ownership always stays with the owner.

The lease's stop func, `Commit`, and `Rollback` all wait for an in-flight
adopter to release before proceeding, so the owner never uses or ends the TX
concurrently with an adopted request; `sqlpro.CtxAdopted(ctx)` reports whether
a context runs adopted — for callers that must refuse work which cannot
complete inside a leased TX (e.g. waiting on effects only visible after the
owner commits).

Nesting stays illegal everywhere else: `ExecTX` on a non-adopted active
transaction still errors.

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
