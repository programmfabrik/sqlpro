package sqlpro

import (
	"context"
	"log"
)

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

	// In case of write mode tx for SQLITE driver
	// There's the need to start it as immediate so it gets a lock
	// Not implemented in driver, therefore this raw SQL workaround
	// Lock, so we can safely do the sqlite3 ROLLBACK / BEGIN below
	if wMode && db.Driver == SQLITE3 {
		db2.txBeginMtx.Lock()
	}

	db2.sqlTx, err = db.sqlDB.BeginTx(ctx, nil)
	if err != nil {
		if wMode && db.Driver == SQLITE3 {
			db2.txBeginMtx.Unlock()
		}
		return nil, err
	}

	// Set flag so we know if to allow write operations
	db2.txWriteMode = wMode

	if wMode && db.Driver == SQLITE3 {
		_, err = db2.sqlTx.ExecContext(ctx, "ROLLBACK; BEGIN IMMEDIATE")
		if err != nil {
			db2.txBeginMtx.Unlock()
			return nil, err
		}
		db2.txBeginMtx.Unlock()
	}

	db2.db = db2.sqlTx

	// debug.PrintStack()

	// pflib.Pln("[%p] BEGIN #%d %s", db.sqlDB, db2.transID, aurora.Blue(fmt.Sprintf("%p", db2.sqlTx)))

	if db.DebugExec || db.Debug {
		log.Printf("%s BEGIN: %s sql.DB: %p", db, &db2, db.sqlDB)
	}

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

	// pflib.Pln("[%p] COMMIT #%d %s", db.sqlDB, db.transID, aurora.Blue(fmt.Sprintf("%p", db.sqlTx)))

	// if db.txWriteMode {
	// 	log.Printf("COMMIT WRITE #%d took %s", db.transID, time.Since(db.txStart))
	// }

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

	// debug.PrintStack()
	// pflib.Pln("[%p] ROLLBACK #%d %s", db.sqlDB, db.transID, aurora.Blue(fmt.Sprintf("%p", db.sqlTx)))

	// if db.txWriteMode {
	// 	log.Printf("ROLLBACK WRITE #%d took %s", db.transID, time.Since(db.txStart))
	// }

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
