package sqlpro

import (
	"context"
	"database/sql"

	"fmt"
	"log"
	"sync"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/stdlib"
)

// txBeginContext starts a new transaction, this panics if the wrapper was not
// initialized using "Open" it gets passed a flag which states if there will be
// any writes.
func (db3 *db) txBeginContext(ctx context.Context, conn *sql.Conn, topts *sql.TxOptions) (dbPtr *db, err error) {

	if db3.sqlDB == nil {
		panic("sqlpro.DB.Begin: The wrapper must be created using Open. The wrapper does not have access to the underlying sql.DB handle.")
	}
	if db3.sqlTx != nil {
		panic("sqlpro.DB.Begin: Unable to call Begin on a Transaction.")
	}

	db2 := *db3
	db2.txExecQueryMtx = &sync.Mutex{}

	wMode := topts == nil || !topts.ReadOnly

	// In case of write mode tx for SQLITE driver There's the need to start it
	// as immediate so it gets a lock Not implemented in driver, therefore this
	// raw SQL workaround Lock, so we can safely do the sqlite3 ROLLBACK / BEGIN
	// below
	if wMode && db3.driver == SQLITE3 {
		db2.txBeginMtx.Lock()
	}

	if conn != nil {
		db2.sqlTx, err = conn.BeginTx(ctx, topts)
	} else {
		db2.sqlTx, err = db3.sqlDB.BeginTx(ctx, topts)
	}
	if err != nil {
		return nil, err
	}
	db2.db = db2.sqlTx

	if err != nil {
		if wMode && db3.driver == SQLITE3 {
			db2.txBeginMtx.Unlock()
		}
		return nil, err
	}

	// Set flag so we know if to allow write operations
	db2.txWriteMode = wMode

	if wMode && db3.driver == SQLITE3 {
		_, err = db2.db.ExecContext(ctx, "ROLLBACK; BEGIN IMMEDIATE")
		if err != nil {
			db2.txBeginMtx.Unlock()
			return nil, err
		}
		db2.txBeginMtx.Unlock()
	}

	// debug.PrintStack()

	// pflib.Pln("[%p] BEGIN #%d %s", db.sqlDB, db2.transID, aurora.Blue(fmt.Sprintf("%p", db2.sqlTx)))

	if db3.DebugExec || db3.Debug {
		log.Printf("%s BEGIN: %s sql.DB: %p", db3, &db2, db3.sqlDB)
	}

	return &db2, nil
}

func (db2 *db) pgxConn() *pgx.Conn {
	if db2.driver != POSTGRES {
		return nil
	}
	if db2.driverConn == nil {
		return nil
	}
	stdConn, ok := db2.driverConn.(*stdlib.Conn)
	if !ok {
		panic(fmt.Errorf("CopyFrom: need PGX driver, but have %T", stdConn))
	}
	return stdConn.Conn()
}

// Begin starts a new transaction, (read-write mode)
func (db2 *db) Begin() (TX, error) {
	return db2.txBeginContext(context.Background(), nil, nil)
}

func (db2 *db) Driver() dbDriver {
	return db2.driver
}

// BeginRead starts a new transaction, read-only mode
func (db2 *db) BeginRead() (TX, error) {
	return db2.txBeginContext(context.Background(), nil, &sql.TxOptions{ReadOnly: true})
}

// Begin starts a new transaction, (read-write mode)
func (db2 *db) BeginContext(ctx context.Context, opts *sql.TxOptions) (TX, error) {
	return db2.txBeginContext(ctx, nil, opts)
}

func (db2 *db) Commit() error {
	if db2.sqlTx == nil {
		panic("sqlpro.DB.Commit: Unable to call Commit without Transaction.")
	}

	if db2.DebugExec || db2.Debug {
		log.Printf("%s COMMIT sql.DB: %p", db2, db2.sqlDB)
	}

	// pflib.Pln("[%p] COMMIT #%d %s", db.sqlDB, db.transID, aurora.Blue(fmt.Sprintf("%p", db.sqlTx)))

	// if db.txWriteMode {
	// 	log.Printf("COMMIT WRITE #%d took %s", db.transID, time.Since(db.txStart))
	// }

	err := db2.sqlTx.Commit()
	db2.sqlTx = nil

	if err != nil {
		return err
	}

	for _, f := range db2.txAfterCommit {
		f()
	}

	return nil
}

func (db2 *db) Rollback() error {
	if db2.sqlTx == nil {
		panic("sqlpro.DB.Rollback: Unable to call Rollback without Transaction.")
	}

	if db2.DebugExec || db2.Debug {
		log.Printf("%s ROLLBACK", db2)
	}

	// debug.PrintStack()
	// pflib.Pln("[%p] ROLLBACK #%d %s", db.sqlDB, db.transID, aurora.Blue(fmt.Sprintf("%p", db.sqlTx)))

	// if db.txWriteMode {
	// 	log.Printf("ROLLBACK WRITE #%d took %s", db.transID, time.Since(db.txStart))
	// }

	err := db2.sqlTx.Rollback()
	db2.sqlTx = nil

	if err != nil {
		return err
	}

	for _, f := range db2.txAfterRollback {
		f()
	}

	return nil
}

func (db2 *db) ActiveTX() bool {
	if db2 == nil {
		return false
	}
	return db2.sqlTx != nil
}

func (db2 *db) AfterTransaction(f func()) {
	if db2.sqlTx == nil {
		panic("sqlpro.DB.AfterTransaction: Needs Transaction.")
	}
	db2.AfterCommit(f)
	db2.AfterRollback(f)
}

func (db2 *db) AfterCommit(f func()) {
	if db2.sqlTx == nil {
		panic("sqlpro.DB.AfterCommit: Needs Transaction.")
	}
	db2.txAfterCommit = append(db2.txAfterCommit, f)
}

func (db2 *db) AfterRollback(f func()) {
	if db2.sqlTx == nil {
		panic("sqlpro.DB.AfterRollback: Needs Transaction.")
	}
	db2.txAfterRollback = append(db2.txAfterRollback, f)
}

func (db2 *db) IsWriteMode() bool {
	return db2.txWriteMode
}
