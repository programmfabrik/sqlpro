package main

import (
	"context"
	"errors"
	"fmt"

	"github.com/programmfabrik/sqlpro"
)

type ledger struct {
	ID   int64  `db:"id,pk,omitempty"`
	Memo string `db:"memo"`
	Amt  int64  `db:"amt"`
}

// transactionExample shows ExecTX, the recommended way to run work in a
// transaction: it opens a dedicated connection, hands a tx-carrying context to
// the job, commits on success and rolls back on error or panic. Inside the job
// you obtain the transaction with sqlpro.CtxTX(ctx) and use it like a DB.
func transactionExample(ctx context.Context, db sqlpro.DB) error {
	if err := db.Exec(`CREATE TABLE ledger(
		id   INTEGER PRIMARY KEY AUTOINCREMENT,
		memo TEXT NOT NULL,
		amt  INTEGER)`); err != nil {
		return err
	}

	// Committing transaction, with lifecycle hooks. BeforeCommit runs while the
	// tx is still open (and can still fail it); AfterCommit/AfterRollback/
	// AfterTransaction run afterwards for side effects (cache busting, logging).
	err := db.ExecTX(ctx, func(ctx context.Context) error {
		tx := sqlpro.CtxTX(ctx)

		tx.BeforeCommit(func() error { fmt.Println("  hook: before commit"); return nil })
		tx.AfterCommit(func() { fmt.Println("  hook: after commit") })
		tx.AfterRollback(func() { fmt.Println("  hook: after rollback") })
		tx.AfterTransaction(func() { fmt.Println("  hook: after transaction") })

		return tx.InsertBulk("ledger", []*ledger{
			{Memo: "opening", Amt: 100},
			{Memo: "coffee", Amt: -3},
		})
	}, nil)
	if err != nil {
		return err
	}

	// Rolling back transaction: returning an error from the job discards every
	// write made inside it.
	wantErr := errors.New("simulated failure")
	err = db.ExecTX(ctx, func(ctx context.Context) error {
		tx := sqlpro.CtxTX(ctx)
		tx.AfterRollback(func() { fmt.Println("  hook: after rollback (rollback case)") })
		if err := tx.Insert("ledger", &ledger{Memo: "should vanish", Amt: 9}); err != nil {
			return err
		}
		return wantErr // -> rollback
	}, nil)
	if !errors.Is(err, wantErr) {
		return fmt.Errorf("expected the simulated error, got %v", err)
	}

	var count int64
	if err := db.Query(&count, "SELECT count(*) FROM ledger"); err != nil {
		return err
	}
	fmt.Printf("ledger rows after commit+rollback: %d (the rolled-back row is gone)\n", count)

	// BeginRead opens an explicit read-only transaction for consistent reads;
	// remember to Commit (or Rollback) it. ExecTX is preferred for writes.
	rtx, err := db.BeginRead()
	if err != nil {
		return err
	}
	var memos []string
	if err := rtx.Query(&memos, "SELECT memo FROM ledger ORDER BY id"); err != nil {
		_ = rtx.Rollback()
		return err
	}
	if err := rtx.Commit(); err != nil {
		return err
	}
	fmt.Printf("read tx saw memos: %v\n", memos)
	return nil
}

// introspectionExample shows the small helper methods.
func introspectionExample(ctx context.Context, db sqlpro.DB) error {
	version, err := db.Version()
	if err != nil {
		return err
	}
	name, err := db.Name()
	if err != nil {
		return err
	}
	fmt.Printf("driver version: %s\n", version)
	fmt.Printf("database name: %s\n", name)

	// ExecContextRowsAffected returns rows affected (and the last insert id where
	// the driver supports it).
	affected, _, err := db.ExecContextRowsAffected(ctx, "UPDATE ledger SET amt = amt WHERE amt < 0")
	if err != nil {
		return err
	}
	fmt.Printf("rows touched by no-op update: %d\n", affected)

	// db.DB() exposes the underlying *sql.DB for anything sqlpro does not wrap.
	if err := db.DB().PingContext(ctx); err != nil {
		return err
	}
	fmt.Println("underlying *sql.DB ping ok")
	return nil
}
