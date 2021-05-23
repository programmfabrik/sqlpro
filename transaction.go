package sqlpro

import (
	"context"
	"log"
	"sync"
)

// var transID = 0
var txBeginMutex = sync.Mutex{}

// txBegin starts a new transaction, this panics if
// the wrapper was not initialized using "Open"
// it gets passed a flag which states if there will be any writes
func (db *DB) txBeginContext(ctx context.Context, wMode bool) (*DB, error) {
	var (
		err error
	)

	if db.sqlDB == nil {
		panic("sqlpro.DB.Begin: The wrapper must be created using Open. The wrapper does not have access to the underlying sql.DB handle.")
	}
	if db.sqlTx != nil {
		panic("sqlpro.DB.Begin: Unable to call Begin on a Transaction.")
	}

	db2 := *db

	// Lock, so we can safely do the sqlite3 ROLLBACK / BEGIN below

	if wMode {
		txBeginMutex.Lock()
	}

	db2.sqlTx, err = db.sqlDB.BeginTx(ctx, nil)
	if err != nil {
		if wMode {
			txBeginMutex.Unlock()
		}
		return nil, err
	}

	// Set flag so we allow or not write operations
	db2.txWriteMode = wMode

	// If tx starts in write mode, special treatment may be in place
	if wMode {
		if db.Driver == supportedSQLiteDriver {
			_, err = db2.sqlTx.ExecContext(ctx, "ROLLBACK; BEGIN IMMEDIATE")
			if err != nil {
				if wMode {
					txBeginMutex.Unlock()
				}
				return nil, err
			}
		}
		txBeginMutex.Unlock()
	}

	db2.db = db2.sqlTx

	return &db2, nil
}

// Begin starts a new transaction, (read-write mode)
func (db *DB) Begin() (*DB, error) {
	return db.txBeginContext(context.Background(), true)
}

// BeginRead starts a new transaction, read-only mode
func (db *DB) BeginRead() (*DB, error) {
	return db.txBeginContext(context.Background(), false)
}

// Begin starts a new transaction, (read-write mode)
func (db *DB) BeginContext(ctx context.Context) (*DB, error) {
	return db.txBeginContext(ctx, true)
}

// BeginRead starts a new transaction, read-only mode
func (db *DB) BeginReadContext(ctx context.Context) (*DB, error) {
	return db.txBeginContext(ctx, false)
}

func (db *DB) Commit() error {
	if db.sqlTx == nil {
		panic("sqlpro.DB.Commit: Unable to call Commit without Transaction.")
	}

	defer func() {
		db.sqlTx = nil
	}()

	if db.DebugExec || db.Debug {
		log.Printf("%s COMMIT sql.DB: %p", db, db.sqlDB)
	}

	err := db.sqlTx.Commit()
	if err != nil {
		return err
	}

	for _, f := range db.txAfterCommit {
		f()
	}

	return nil
}

func (db *DB) Rollback() error {
	if db.sqlTx == nil {
		panic("sqlpro.DB.Rollback: Unable to call Rollback without Transaction.")
	}

	defer func() {
		db.sqlTx = nil
	}()

	if db.DebugExec || db.Debug {
		log.Printf("%s ROLLBACK", db)
	}

	err := db.sqlTx.Rollback()
	if err != nil {
		return err
	}

	for _, f := range db.txAfterRollback {
		f()
	}

	return nil
}

func (db *DB) ActiveTX() bool {
	if db == nil {
		return false
	}
	return db.sqlTx != nil
}

func (db *DB) AfterCommit(f func()) {
	if db.sqlTx == nil {
		panic("sqlpro.DB.AfterCommit: Needs Transaction.")
	}
	db.txAfterCommit = append(db.txAfterCommit, f)
}

func (db *DB) AfterRollback(f func()) {
	if db.sqlTx == nil {
		panic("sqlpro.DB.AfterRollback: Needs Transaction.")
	}
	db.txAfterRollback = append(db.txAfterRollback, f)
}

func (db *DB) IsWriteMode() bool {
	return db.txWriteMode
}
