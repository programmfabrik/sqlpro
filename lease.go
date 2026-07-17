package sqlpro

import (
	"context"
	"crypto/rand"
	"database/sql"
	"encoding/hex"
	"fmt"
	"os"
	"runtime/debug"
	"sync"
)

// Leased transactions: an open write TX can be handed to another goroutine —
// e.g. a re-entrant HTTP request made by a plugin callback while the owning
// request is parked waiting for that callback — under an unguessable id. The
// adopter joins the TX via AdoptTX; ExecTX calls on an adopted TX run the job
// directly on the owner's transaction (execTXJoin) instead of failing with
// "unable to nest transaction". There is no savepoint bracket: a failed
// write-intent join marks the whole transaction failed, so the owner's Commit
// rolls back instead — the leased TX fails as a unit on adopter error.
// Commit/Rollback stay with the owner; they, and the lease's stop func, wait
// for an in-flight adopter before proceeding and invalidate all outstanding
// leases of the TX.

type leaseEntry struct {
	tx      *db
	userMu  sync.Mutex // serializes adopters; held for the adopter's whole window
	stopped bool       // set by leaseStop under userMu; AdoptTX re-checks it
}

var (
	leaseMtx sync.Mutex
	leases   = map[string]*leaseEntry{}
)

// Lease registers the open write TX under a crypto-random id and returns the
// id plus a stop func which invalidates it (idempotent; Commit and Rollback
// invalidate all leases of the TX as well). The id is a capability: whoever
// presents it to AdoptTX joins this transaction — hand it out accordingly.
// stop blocks until an in-flight adopter has released, so after it returns
// the owner is free to use and end the TX.
func (db2 *db) Lease() (id string, stop func()) {
	if db2 == nil || db2.sqlTx == nil {
		panic("sqlpro.TX.Lease: no open transaction")
	}
	if !db2.txWriteMode {
		panic("sqlpro.TX.Lease: transaction is not in write mode")
	}
	buf := make([]byte, 32)
	if _, err := rand.Read(buf); err != nil {
		panic(fmt.Errorf("sqlpro.TX.Lease: %w", err))
	}
	id = hex.EncodeToString(buf)

	leaseMtx.Lock()
	leases[id] = &leaseEntry{tx: db2}
	leaseMtx.Unlock()
	db2.leaseIDs = append(db2.leaseIDs, id)

	return id, func() { leaseStop(id) }
}

// leaseStop invalidates the given leases and waits for in-flight adopters:
// the ids are deleted first so no new adoption can resolve them, then each
// entry's adopter mutex is acquired, which blocks until an adopted request
// still executing on the TX has released. After leaseStop returns the TX has
// no active adopter.
func leaseStop(ids ...string) {
	entries := make([]*leaseEntry, 0, len(ids))
	leaseMtx.Lock()
	for _, id := range ids {
		if e := leases[id]; e != nil {
			entries = append(entries, e)
			delete(leases, id)
		}
	}
	leaseMtx.Unlock()
	for _, e := range entries {
		// Acquiring the mutex is the wait: an in-flight adopter holds it
		// until release. The flag catches an adopter that resolved the entry
		// before the delete above and is still waiting for the mutex.
		e.userMu.Lock()
		e.stopped = true
		e.userMu.Unlock()
	}
}

// leaseEnd invalidates all outstanding leases of the TX and waits for an
// in-flight adopter; called by Commit and Rollback before they end the
// transaction so no adoption can be active or start while the TX ends.
func (db2 *db) leaseEnd() {
	if len(db2.leaseIDs) == 0 {
		return
	}
	leaseStop(db2.leaseIDs...)
	db2.leaseIDs = nil
}

// AdoptTX resolves a leased transaction and returns a ctx carrying it, marked
// so that ExecTX joins it directly instead of refusing to nest. It serializes
// adopters: a second AdoptTX for the same lease blocks until the first calls
// release. release is idempotent and must be called when the adopter is done.
// An unknown, stopped, ended, or failed lease is an error.
func AdoptTX(ctx context.Context, id string) (ctx2 context.Context, release func(), err error) {
	leaseMtx.Lock()
	entry := leases[id]
	leaseMtx.Unlock()
	if entry == nil {
		return ctx, func() {}, fmt.Errorf("sqlpro.AdoptTX: unknown or ended lease")
	}
	entry.userMu.Lock()
	// Re-check under the adopter mutex: the lease may have been stopped — and
	// its stop func returned — while this adopter was waiting for the mutex.
	if entry.stopped {
		entry.userMu.Unlock()
		return ctx, func() {}, fmt.Errorf("sqlpro.AdoptTX: unknown or ended lease")
	}
	if !entry.tx.ActiveTX() {
		entry.userMu.Unlock()
		return ctx, func() {}, fmt.Errorf("sqlpro.AdoptTX: leased transaction has ended")
	}
	if entry.tx.txFailed != nil {
		entry.userMu.Unlock()
		return ctx, func() {}, fmt.Errorf("sqlpro.AdoptTX: leased transaction has failed: %w", entry.tx.txFailed)
	}
	ctx = context.WithValue(ctx, ctxAdoptedKey{}, true)
	return CtxWithTX(ctx, entry.tx), sync.OnceFunc(entry.userMu.Unlock), nil
}

type ctxAdoptedKey struct{}

func ctxAdopted(ctx context.Context) bool {
	v, _ := ctx.Value(ctxAdoptedKey{}).(bool)
	return v
}

// CtxAdopted reports whether ctx carries a transaction joined via AdoptTX.
// Callers can use this to refuse work that cannot complete inside a leased
// TX — e.g. waiting on side effects only visible after the owner commits.
func CtxAdopted(ctx context.Context) bool {
	return ctxAdopted(ctx)
}

// execTXJoin runs job directly on the adopted TX — no nested BEGIN, no
// savepoint. Commit stays with the owner. A failed (or panicked) write-intent
// job (opts nil or not ReadOnly) may have left partial writes on the TX, so
// it marks the transaction failed: the owner's Commit refuses and rolls back
// — the leased TX fails as a whole on adopter error. A read-only join's error
// is only returned; it leaves the TX healthy.
func execTXJoin(ctx context.Context, tx *db, job func(ctx context.Context) error, opts *sql.TxOptions) (err error) {
	if tx.txFailed != nil {
		return fmt.Errorf("sqlpro.ExecTX: adopted transaction has failed: %w", tx.txFailed)
	}
	err = func() (err error) {
		defer func() {
			r := recover()
			if r == nil {
				return
			}
			err = fmt.Errorf("sqlpro.ExecTX: panic caught: %v", r)
			fmt.Fprint(os.Stderr, err.Error()+"\n")
			debug.PrintStack()
		}()
		return job(ctx)
	}()
	if err != nil && (opts == nil || !opts.ReadOnly) {
		tx.txFailed = err
	}
	return err
}
