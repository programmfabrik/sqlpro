package sqlpro

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"os"
	"runtime/debug"
	"sync"
)

// Leased transactions: an open write TX can be handed to another goroutine —
// e.g. a re-entrant HTTP request made by a plugin callback while the owning
// request is parked waiting for that callback — under an unguessable id. The
// adopter joins the TX via AdoptTX; ExecTX calls on an adopted TX run as
// savepoints (execTXSavepoint) instead of failing with "unable to nest
// transaction". Commit/Rollback stay with the owner and invalidate all
// outstanding leases of the TX.

type leaseEntry struct {
	tx     *db
	userMu sync.Mutex // serializes adopters; the owner is parked while leased
}

var (
	leaseMtx sync.Mutex
	leases   = map[string]*leaseEntry{}
)

// Lease registers the open write TX under a crypto-random id and returns the
// id plus a stop func which invalidates it (idempotent; Commit and Rollback
// invalidate all leases of the TX as well). The id is a capability: whoever
// presents it to AdoptTX joins this transaction — hand it out accordingly.
// The owner must not use or end the TX while an adopter is active; typically
// it is blocked waiting for the adopter's work for the whole lease window.
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

func leaseStop(ids ...string) {
	leaseMtx.Lock()
	defer leaseMtx.Unlock()
	for _, id := range ids {
		delete(leases, id)
	}
}

// leaseEnd invalidates all outstanding leases of the TX; called by Commit and
// Rollback before they end the transaction so no new adoption can start.
func (db2 *db) leaseEnd() {
	if len(db2.leaseIDs) == 0 {
		return
	}
	leaseStop(db2.leaseIDs...)
	db2.leaseIDs = nil
}

// AdoptTX resolves a leased transaction and returns a ctx carrying it, marked
// so that ExecTX joins it via savepoints instead of refusing to nest. It
// serializes adopters: a second AdoptTX for the same lease blocks until the
// first calls release. release is idempotent and must be called when the
// adopter is done. An unknown, stopped, or already-ended lease is an error.
func AdoptTX(ctx context.Context, id string) (ctx2 context.Context, release func(), err error) {
	leaseMtx.Lock()
	entry := leases[id]
	leaseMtx.Unlock()
	if entry == nil {
		return ctx, func() {}, fmt.Errorf("sqlpro.AdoptTX: unknown or ended lease")
	}
	entry.userMu.Lock()
	if !entry.tx.ActiveTX() {
		entry.userMu.Unlock()
		return ctx, func() {}, fmt.Errorf("sqlpro.AdoptTX: leased transaction has ended")
	}
	ctx = context.WithValue(ctx, ctxAdoptedKey{}, true)
	return CtxWithTX(ctx, entry.tx), sync.OnceFunc(entry.userMu.Unlock), nil
}

type ctxAdoptedKey struct{}

func ctxAdopted(ctx context.Context) bool {
	v, _ := ctx.Value(ctxAdoptedKey{}).(bool)
	return v
}

// execTXSavepoint runs job inside a SAVEPOINT on the adopted TX: released on
// success, rolled back to on error or panic — so a failed adopter leaves the
// owner's transaction exactly as it was. Commit stays with the owner.
func execTXSavepoint(ctx context.Context, tx *db, job func(ctx context.Context) error) (err error) {
	tx.savepointN++
	name := fmt.Sprintf("sqlpro_sp_%d", tx.savepointN)

	err = tx.ExecContext(ctx, "SAVEPOINT "+name)
	if err != nil {
		return fmt.Errorf("sqlpro.ExecTX: savepoint: %w", err)
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
	if err != nil {
		if err2 := tx.ExecContext(ctx, "ROLLBACK TO SAVEPOINT "+name); err2 != nil {
			return fmt.Errorf("%w; rollback to savepoint: %w", err, err2)
		}
		_ = tx.ExecContext(ctx, "RELEASE SAVEPOINT "+name)
		return err
	}
	err = tx.ExecContext(ctx, "RELEASE SAVEPOINT "+name)
	if err != nil {
		return fmt.Errorf("sqlpro.ExecTX: release savepoint: %w", err)
	}
	return nil
}
