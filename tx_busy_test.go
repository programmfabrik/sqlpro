package sqlpro

import (
	"context"
	"net/url"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

// A write ExecTX that cannot get the SQLITE3 write lock must surface the busy
// error within the busy timeout — not deadlock in the deferred conn.Close() on
// a leaked sql.Tx (fylr ticket #80077) — and the handle must stay usable.
func TestSqliteWriteTXBusyReturnsError(t *testing.T) {
	file := filepath.Join(t.TempDir(), "busy.db")
	qv := url.Values{}
	qv.Add("_pragma", "busy_timeout(100)")
	qv.Add("_pragma", "journal_mode(WAL)")

	open := func() DB {
		d, err := Open("sqlite", file+"?"+qv.Encode())
		assert.NoError(t, err)
		return d
	}
	holder, contender := open(), open()
	defer holder.Close()
	defer contender.Close()

	assert.NoError(t, contender.Exec(`CREATE TABLE busy_t(x INTEGER)`))

	// hold the sqlite write lock (write TXs run BEGIN IMMEDIATE)
	blockTX, err := holder.Begin()
	assert.NoError(t, err)

	done := make(chan error, 1)
	go func() {
		done <- contender.ExecTX(context.Background(), func(ctx context.Context) error {
			return CtxTX(ctx).Exec(`INSERT INTO busy_t(x) VALUES (2)`)
		}, nil)
	}()

	select {
	case err := <-done:
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "locked")
	case <-time.After(10 * time.Second):
		t.Fatal("ExecTX deadlocked on a busy write tx (leaked sql.Tx pins conn.Close)")
	}

	assert.NoError(t, blockTX.Rollback())

	// the contender handle must not be wedged after the failed begin
	err = contender.ExecTX(context.Background(), func(ctx context.Context) error {
		return CtxTX(ctx).Exec(`INSERT INTO busy_t(x) VALUES (3)`)
	}, nil)
	assert.NoError(t, err)
}
