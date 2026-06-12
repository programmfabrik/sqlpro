package sqlpro

// Transaction and ExecTX paths tested against the fake driver (fakedb_test.go).
// The fake records BEGIN/COMMIT/ROLLBACK and all statements, so the
// driver-specific transaction setup (SQLite BEGIN IMMEDIATE / PRAGMA, Postgres
// lock_timeout) and the hook/error flows can be asserted without a real DB.

import (
	"context"
	"database/sql"
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestFakeBeginCommit(t *testing.T) {
	db, backend := newFakeSqlPro(t, SQLITE3)

	tx, err := db.Begin()
	require.NoError(t, err)
	assert.True(t, tx.ActiveTX())
	assert.True(t, tx.IsWriteMode())
	assert.False(t, db.ActiveTX(), "tx state lives on the copy, not the DB handle")

	// SQLite write mode upgrades the lazy BEGIN to an immediate one
	assert.Equal(t, []string{"ROLLBACK; BEGIN IMMEDIATE"}, backend.recorded("exec"))

	require.NoError(t, tx.Commit())
	assert.False(t, tx.ActiveTX())
	assert.Equal(t, []string{"COMMIT"}, backend.recorded("commit"))
}

func TestFakeBeginPostgresNoImmediate(t *testing.T) {
	db, backend := newFakeSqlPro(t, POSTGRES)

	tx, err := db.Begin()
	require.NoError(t, err)
	assert.Empty(t, backend.recorded("exec"), "no BEGIN IMMEDIATE workaround on postgres")
	require.NoError(t, tx.Rollback())
}

func TestFakeBeginContextReadOnly(t *testing.T) {
	db, _ := newFakeSqlPro(t, SQLITE3)

	tx, err := db.BeginContext(context.Background(), &sql.TxOptions{ReadOnly: true})
	require.NoError(t, err)
	assert.False(t, tx.IsWriteMode())
	require.NoError(t, tx.Rollback())
}

func TestFakeBeginErrors(t *testing.T) {
	db, backend := newFakeSqlPro(t, SQLITE3)

	backend.beginErr = fmt.Errorf("no conn")
	_, err := db.Begin()
	require.Error(t, err)
	backend.beginErr = nil

	// the BEGIN IMMEDIATE upgrade can fail, too
	backend.queueExecErr(fmt.Errorf("locked"))
	_, err = db.Begin()
	require.Error(t, err)
	assert.Contains(t, err.Error(), "locked")
}

func TestFakeBeginPanics(t *testing.T) {
	assert.Panics(t, func() {
		wrapper := newSqlPro(nil)
		_, _ = wrapper.Begin()
	}, "Begin without Open")

	dbc, _ := newFakeSqlPro(t, SQLITE3)
	tx, err := dbc.Begin()
	require.NoError(t, err)
	defer func() { _ = tx.Rollback() }()
	assert.Panics(t, func() {
		_, _ = tx.(*db).Begin()
	}, "Begin on a transaction")
}

func TestFakeCommitRollbackPanicsWithoutTX(t *testing.T) {
	db, _ := newFakeSqlPro(t, SQLITE3)

	assert.Panics(t, func() { _ = db.Commit() })
	assert.Panics(t, func() { _ = db.Rollback() })
	assert.Panics(t, func() { db.BeforeCommit(func() error { return nil }) })
	assert.Panics(t, func() { db.AfterCommit(func() {}) })
	assert.Panics(t, func() { db.AfterRollback(func() {}) })
	assert.Panics(t, func() { db.AfterTransaction(func() {}) })
}

// --- hooks ---------------------------------------------------------------------

func TestFakeCommitHooks(t *testing.T) {
	db, _ := newFakeSqlPro(t, SQLITE3)

	tx, err := db.Begin()
	require.NoError(t, err)

	order := []string{}
	tx.BeforeCommit(func() error { order = append(order, "before"); return nil })
	tx.AfterCommit(func() { order = append(order, "after") })
	tx.AfterRollback(func() { order = append(order, "rollback") })

	require.NoError(t, tx.Commit())
	assert.Equal(t, []string{"before", "after"}, order, "rollback hook must not run on commit")
}

func TestFakeBeforeCommitError(t *testing.T) {
	db, backend := newFakeSqlPro(t, SQLITE3)

	tx, err := db.Begin()
	require.NoError(t, err)

	afterCommitRan := false
	rollbackRan := false
	tx.BeforeCommit(func() error { return fmt.Errorf("veto") })
	tx.AfterCommit(func() { afterCommitRan = true })
	tx.AfterRollback(func() { rollbackRan = true })

	err = tx.Commit()
	require.Error(t, err)
	assert.Contains(t, err.Error(), "beforeCommit hook: veto")
	assert.False(t, afterCommitRan)
	assert.True(t, rollbackRan, "failed BeforeCommit rolls the tx back")
	assert.Equal(t, []string{"ROLLBACK"}, backend.recorded("rollback"))
}

func TestFakeBeforeCommitErrorAndRollbackError(t *testing.T) {
	db, backend := newFakeSqlPro(t, SQLITE3)

	tx, err := db.Begin()
	require.NoError(t, err)
	tx.BeforeCommit(func() error { return fmt.Errorf("veto") })
	backend.rollbackErr = fmt.Errorf("rb broken")

	err = tx.Commit()
	require.Error(t, err)
	assert.Contains(t, err.Error(), "veto")
	assert.Contains(t, err.Error(), "rb broken")
}

func TestFakeCommitDriverError(t *testing.T) {
	db, backend := newFakeSqlPro(t, SQLITE3)

	tx, err := db.Begin()
	require.NoError(t, err)
	afterCommitRan := false
	tx.AfterCommit(func() { afterCommitRan = true })
	backend.commitErr = fmt.Errorf("commit broken")

	err = tx.Commit()
	require.Error(t, err)
	assert.False(t, afterCommitRan, "AfterCommit must not run on commit error")
}

func TestFakeRollback(t *testing.T) {
	db, backend := newFakeSqlPro(t, SQLITE3)

	tx, err := db.Begin()
	require.NoError(t, err)
	rollbackRan := false
	tx.AfterRollback(func() { rollbackRan = true })
	require.NoError(t, tx.Rollback())
	assert.True(t, rollbackRan)
	assert.Equal(t, []string{"ROLLBACK"}, backend.recorded("rollback"))

	// rollback driver error: AfterRollback hooks must not run
	tx2, err := db.Begin()
	require.NoError(t, err)
	rollbackRan = false
	tx2.AfterRollback(func() { rollbackRan = true })
	backend.rollbackErr = fmt.Errorf("rb broken")
	assert.Error(t, tx2.Rollback())
	assert.False(t, rollbackRan)
}

func TestFakeAfterTransaction(t *testing.T) {
	db, _ := newFakeSqlPro(t, SQLITE3)

	// runs once on commit ...
	tx, err := db.Begin()
	require.NoError(t, err)
	calls := 0
	tx.AfterTransaction(func() { calls++ })
	require.NoError(t, tx.Commit())
	assert.Equal(t, 1, calls)

	// ... and once on rollback
	tx, err = db.Begin()
	require.NoError(t, err)
	calls = 0
	tx.AfterTransaction(func() { calls++ })
	require.NoError(t, tx.Rollback())
	assert.Equal(t, 1, calls)
}

func TestFakeActiveTX(t *testing.T) {
	var nilDB *db
	assert.False(t, nilDB.ActiveTX(), "nil handle has no active TX")
}

// --- ExecTX --------------------------------------------------------------------

func TestFakeExecTXSqlite(t *testing.T) {
	dbc, backend := newFakeSqlPro(t, SQLITE3)

	err := dbc.ExecTX(context.Background(), func(ctx context.Context) error {
		tx := CtxTX(ctx)
		require.True(t, tx.ActiveTX(), "TX injected into ctx")
		assert.NotNil(t, tx.(*db).driverConn, "raw driver conn captured")
		return tx.Exec("DELETE FROM t")
	}, nil)
	require.NoError(t, err)

	assert.Equal(t, []string{
		"ROLLBACK; BEGIN IMMEDIATE",
		"PRAGMA defer_foreign_keys='ON'",
		"DELETE FROM t",
	}, backend.recorded("exec"))
	assert.Equal(t, []string{"COMMIT"}, backend.recorded("commit"))
}

func TestFakeExecTXPostgres(t *testing.T) {
	db, backend := newFakeSqlPro(t, POSTGRES)

	err := db.ExecTX(context.Background(), func(ctx context.Context) error {
		return CtxTX(ctx).Exec("DELETE FROM t")
	}, nil)
	require.NoError(t, err)

	assert.Equal(t, []string{
		`SET LOCAL lock_timeout = '300s'`,
		"DELETE FROM t",
	}, backend.recorded("exec"))
}

func TestFakeExecTXReadOnlySkipsSetup(t *testing.T) {
	db, backend := newFakeSqlPro(t, POSTGRES)

	err := db.ExecTX(context.Background(), func(ctx context.Context) error {
		return nil
	}, &sql.TxOptions{ReadOnly: true})
	require.NoError(t, err)
	assert.Empty(t, backend.recorded("exec"), "no lock_timeout in read-only mode")
}

func TestFakeExecTXSetupError(t *testing.T) {
	db, backend := newFakeSqlPro(t, POSTGRES)

	backend.queueExecErr(fmt.Errorf("no lock"))
	jobRan := false
	err := db.ExecTX(context.Background(), func(ctx context.Context) error {
		jobRan = true
		return nil
	}, nil)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "no lock")
	assert.False(t, jobRan)
	assert.Equal(t, []string{"ROLLBACK"}, backend.recorded("rollback"))
}

func TestFakeExecTXContextDone(t *testing.T) {
	db, _ := newFakeSqlPro(t, SQLITE3)

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	err := db.ExecTX(ctx, func(ctx context.Context) error { return nil }, nil)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "context is done")
}

func TestFakeExecTXNoNesting(t *testing.T) {
	db, _ := newFakeSqlPro(t, SQLITE3)

	err := db.ExecTX(context.Background(), func(ctx context.Context) error {
		return db.ExecTX(ctx, func(ctx context.Context) error { return nil }, nil)
	}, nil)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "unable to nest")
}

func TestFakeExecTXJobError(t *testing.T) {
	db, backend := newFakeSqlPro(t, SQLITE3)

	err := db.ExecTX(context.Background(), func(ctx context.Context) error {
		return fmt.Errorf("job failed")
	}, nil)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "job failed")
	assert.Equal(t, []string{"ROLLBACK"}, backend.recorded("rollback"))
	assert.Empty(t, backend.recorded("commit"))
}

func TestFakeExecTXJobErrorAndRollbackError(t *testing.T) {
	db, backend := newFakeSqlPro(t, SQLITE3)

	backend.rollbackErr = fmt.Errorf("rb broken")
	err := db.ExecTX(context.Background(), func(ctx context.Context) error {
		return fmt.Errorf("job failed")
	}, nil)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "job failed")
	assert.Contains(t, err.Error(), "rb broken")
}

func TestFakeExecTXJobPanic(t *testing.T) {
	db, backend := newFakeSqlPro(t, SQLITE3)

	err := db.ExecTX(context.Background(), func(ctx context.Context) error {
		panic("boom")
	}, nil)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "panic caught: boom")
	assert.Equal(t, []string{"ROLLBACK"}, backend.recorded("rollback"))
}

func TestFakeExecTXCommitError(t *testing.T) {
	db, backend := newFakeSqlPro(t, SQLITE3)

	backend.commitErr = fmt.Errorf("commit broken")
	err := db.ExecTX(context.Background(), func(ctx context.Context) error { return nil }, nil)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "commit:")
}

func TestFakeExecTXInsertBulkNeedsRealPgx(t *testing.T) {
	db, _ := newFakeSqlPro(t, POSTGRES)

	type one struct {
		Name string `db:"name"`
	}
	// Inside ExecTX on POSTGRES a driverConn is captured; InsertBulk then wants
	// the pgx COPY FROM fast path and panics on a non-pgx driver. ExecTX
	// converts the panic into an error.
	err := db.ExecTX(context.Background(), func(ctx context.Context) error {
		return CtxTX(ctx).InsertBulk("t", []one{{Name: "a"}})
	}, nil)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "need PGX driver")
}

func TestFakeInsertBulkPostgresWithoutTXFallsBack(t *testing.T) {
	db, backend := newFakeSqlPro(t, POSTGRES)

	type one struct {
		Name string `db:"name"`
	}
	// without ExecTX there is no captured driverConn -> multi-row INSERT
	backend.queueExec(1, 0)
	require.NoError(t, db.InsertBulk("t", []one{{Name: "a"}}))
	st, _ := backend.lastStatement("exec")
	assert.Contains(t, st.sql, "INSERT INTO \"t\"")
}

func TestFakeCtxTX(t *testing.T) {
	// empty context returns a typed nil that is safe to query
	tx := CtxTX(context.Background())
	assert.False(t, tx.ActiveTX())
}
