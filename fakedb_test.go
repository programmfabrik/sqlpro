package sqlpro

// fakedb_test.go provides an in-memory fake database/sql/driver implementation
// in the spirit of the Go stdlib's own fakedb (used to test database/sql
// itself). Unlike the stdlib fake it does not parse SQL: sqlpro generates real
// SQL which no toy parser could execute. Instead the fake is scripted — tests
// enqueue canned query/exec results and assert the statements that reached the
// driver. This allows unit-testing sqlpro without sqlite, postgres or any
// other real database, including the POSTGRES-mode code paths (DOLLAR
// placeholders, RETURNING, lock_timeout) which the sqlite-backed suite cannot
// reach.

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"fmt"
	"io"
	"sync"
	"testing"
	"time"
)

// fakeStatement is one statement recorded by the fake driver.
type fakeStatement struct {
	kind string // "exec", "query", "begin", "commit", "rollback"
	sql  string
	args []driver.Value
}

// fakeQueryResult is a canned response for one Query call.
type fakeQueryResult struct {
	cols []string
	rows [][]driver.Value
	err  error
}

// fakeExecResult is a canned response for one Exec call.
type fakeExecResult struct {
	rowsAffected      int64
	lastInsertID      int64
	err               error // returned by ExecContext itself
	lastInsertIDErr   error // returned by Result.LastInsertId
	rowsAffectedErr   error // returned by Result.RowsAffected
	rowsAffectedPanic bool  // Result.RowsAffected panics (sqlite does this on empty SQL)
}

// fakeBackend is shared by all connections of one fake DB. It records every
// statement and serves canned results from FIFO queues. With empty queues,
// queries return zero rows and execs report 1 affected row plus an
// auto-incremented last-insert-id, so incidental statements (PRAGMA, SET
// LOCAL, BEGIN IMMEDIATE) need no scripting.
type fakeBackend struct {
	mu          sync.Mutex
	statements  []fakeStatement
	queryQ      []fakeQueryResult
	execQ       []fakeExecResult
	beginErr    error
	commitErr   error
	rollbackErr error
	autoID      int64
}

func (b *fakeBackend) record(kind, sqlS string, args []driver.Value) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.statements = append(b.statements, fakeStatement{kind: kind, sql: sqlS, args: args})
}

func (b *fakeBackend) queueQuery(cols []string, rows [][]driver.Value) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.queryQ = append(b.queryQ, fakeQueryResult{cols: cols, rows: rows})
}

func (b *fakeBackend) queueQueryErr(err error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.queryQ = append(b.queryQ, fakeQueryResult{err: err})
}

func (b *fakeBackend) queueExec(rowsAffected, lastInsertID int64) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.execQ = append(b.execQ, fakeExecResult{rowsAffected: rowsAffected, lastInsertID: lastInsertID})
}

func (b *fakeBackend) queueExecErr(err error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.execQ = append(b.execQ, fakeExecResult{err: err})
}

func (b *fakeBackend) queueExecResult(res fakeExecResult) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.execQ = append(b.execQ, res)
}

// recorded returns the SQL of all recorded statements of the given kinds (all
// statements if no kind is given).
func (b *fakeBackend) recorded(kinds ...string) []string {
	b.mu.Lock()
	defer b.mu.Unlock()
	out := []string{}
	for _, st := range b.statements {
		if len(kinds) == 0 {
			out = append(out, st.sql)
			continue
		}
		for _, k := range kinds {
			if st.kind == k {
				out = append(out, st.sql)
				break
			}
		}
	}
	return out
}

// lastStatement returns the most recent statement of the given kind.
func (b *fakeBackend) lastStatement(kind string) (fakeStatement, bool) {
	b.mu.Lock()
	defer b.mu.Unlock()
	for i := len(b.statements) - 1; i >= 0; i-- {
		if b.statements[i].kind == kind {
			return b.statements[i], true
		}
	}
	return fakeStatement{}, false
}

func (b *fakeBackend) popQuery() (fakeQueryResult, bool) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if len(b.queryQ) == 0 {
		return fakeQueryResult{}, false
	}
	res := b.queryQ[0]
	b.queryQ = b.queryQ[1:]
	return res, true
}

func (b *fakeBackend) popExec() fakeExecResult {
	b.mu.Lock()
	defer b.mu.Unlock()
	if len(b.execQ) == 0 {
		b.autoID++
		return fakeExecResult{rowsAffected: 1, lastInsertID: b.autoID}
	}
	res := b.execQ[0]
	b.execQ = b.execQ[1:]
	return res
}

// --- driver plumbing ---------------------------------------------------------

type fakeDriver struct{}

func (fakeDriver) Open(name string) (driver.Conn, error) {
	return nil, fmt.Errorf("fakeDriver: use sql.OpenDB with a fakeConnector")
}

type fakeConnector struct {
	backend *fakeBackend
}

func (c *fakeConnector) Connect(context.Context) (driver.Conn, error) {
	return &fakeConn{backend: c.backend}, nil
}

func (c *fakeConnector) Driver() driver.Driver { return fakeDriver{} }

type fakeConn struct {
	backend *fakeBackend
}

func (c *fakeConn) Prepare(query string) (driver.Stmt, error) {
	// Exec/Query are served via ExecerContext/QueryerContext; database/sql
	// only falls back to Prepare if those are missing.
	return nil, fmt.Errorf("fakeConn: unexpected Prepare(%q)", query)
}

func (c *fakeConn) Close() error { return nil }

func (c *fakeConn) Begin() (driver.Tx, error) {
	return c.BeginTx(context.Background(), driver.TxOptions{})
}

func (c *fakeConn) BeginTx(ctx context.Context, opts driver.TxOptions) (driver.Tx, error) {
	c.backend.record("begin", "BEGIN", nil)
	c.backend.mu.Lock()
	err := c.backend.beginErr
	c.backend.mu.Unlock()
	if err != nil {
		return nil, err
	}
	return &fakeTx{backend: c.backend}, nil
}

func namedToValues(named []driver.NamedValue) []driver.Value {
	args := make([]driver.Value, len(named))
	for i, nv := range named {
		args[i] = nv.Value
	}
	return args
}

func (c *fakeConn) ExecContext(ctx context.Context, query string, named []driver.NamedValue) (driver.Result, error) {
	c.backend.record("exec", query, namedToValues(named))
	res := c.backend.popExec()
	if res.err != nil {
		return nil, res.err
	}
	return &fakeResult{res: res}, nil
}

func (c *fakeConn) QueryContext(ctx context.Context, query string, named []driver.NamedValue) (driver.Rows, error) {
	c.backend.record("query", query, namedToValues(named))
	res, ok := c.backend.popQuery()
	if !ok {
		return &fakeRows{}, nil
	}
	if res.err != nil {
		return nil, res.err
	}
	return &fakeRows{cols: res.cols, rows: res.rows}, nil
}

type fakeTx struct {
	backend *fakeBackend
}

func (tx *fakeTx) Commit() error {
	tx.backend.record("commit", "COMMIT", nil)
	tx.backend.mu.Lock()
	defer tx.backend.mu.Unlock()
	return tx.backend.commitErr
}

func (tx *fakeTx) Rollback() error {
	tx.backend.record("rollback", "ROLLBACK", nil)
	tx.backend.mu.Lock()
	defer tx.backend.mu.Unlock()
	return tx.backend.rollbackErr
}

type fakeResult struct {
	res fakeExecResult
}

func (r *fakeResult) LastInsertId() (int64, error) {
	if r.res.lastInsertIDErr != nil {
		return 0, r.res.lastInsertIDErr
	}
	return r.res.lastInsertID, nil
}

func (r *fakeResult) RowsAffected() (int64, error) {
	if r.res.rowsAffectedPanic {
		panic("fakeResult: RowsAffected panic requested")
	}
	if r.res.rowsAffectedErr != nil {
		return 0, r.res.rowsAffectedErr
	}
	return r.res.rowsAffected, nil
}

type fakeRows struct {
	cols []string
	rows [][]driver.Value
	next int
}

func (r *fakeRows) Columns() []string { return r.cols }
func (r *fakeRows) Close() error      { return nil }

func (r *fakeRows) Next(dest []driver.Value) error {
	if r.next >= len(r.rows) {
		return io.EOF
	}
	copy(dest, r.rows[r.next])
	r.next++
	return nil
}

// newFakeSqlPro returns a sqlpro wrapper connected to a fake backend,
// replicating the driver-specific wiring of Open (util.go) without going
// through driver registration.
func newFakeSqlPro(t *testing.T, drv dbDriver) (*db, *fakeBackend) {
	t.Helper()

	backend := &fakeBackend{}
	conn := sql.OpenDB(&fakeConnector{backend: backend})
	t.Cleanup(func() { conn.Close() })

	wrapper := newSqlPro(conn)
	wrapper.sqlDB = conn
	wrapper.driver = drv
	wrapper.DSN = "fake:" + string(drv)

	switch drv {
	case POSTGRES:
		wrapper.PlaceholderMode = DOLLAR
		wrapper.UseReturningForLastId = true
		wrapper.SupportsLastInsertId = false
	case SQLITE3:
		wrapper.timeFormat = time.RFC3339Nano
	}
	return wrapper, backend
}
