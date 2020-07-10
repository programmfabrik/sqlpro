package sqlpro

import (
	"log"
	"sync"
)

// var transID = 0
var txBeginMutex = sync.Mutex{}

// txBegin starts a new transaction, this panics if
// the wrapper was not initialized using "Open"
// it gets passed a flag which states if there will be any writes
func (db *DB) txBegin(wMode bool) (*DB, error) {
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

	// db2.transID = transID
	// transID++

	// pflib.Pln("[%p] BEFORE BEGIN #%d %s", db.sqlDB, db2.transID, aurora.Blue(fmt.Sprintf("%p", db2.sqlTx)))

	// Lock, so we can safely do the sqlite3 ROLLBACK / BEGIN below

	if wMode {
		txBeginMutex.Lock()
	}

	db2.sqlTx, err = db.sqlDB.Begin()
	if err != nil {
		return nil, err
	}

	// Set flag so we allow or not write operations
	db2.txWriteMode = wMode

	// If tx starts in write mode, special treatment may be in place
	if wMode {
		switch db.Driver {

		// In case of write mode tx for SQLITE driver
		// There's the need to start it as immediate so it gets a lock
		// Not implemented in driver, therefore this raw SQL workaround
		case SQLITE3:
			// log.Printf("%s IMMEDIATE TX: %s sql.DB: %p", db, &db2, db.sqlDB)
			_, err = db2.sqlTx.Exec("ROLLBACK; BEGIN IMMEDIATE")
			if err != nil {
				return nil, err
			}
		}

		txBeginMutex.Unlock()
	}

	db2.db = db2.sqlTx

	// debug.PrintStack()

	// pflib.Pln("[%p] BEGIN #%d %s", db.sqlDB, db2.transID, aurora.Blue(fmt.Sprintf("%p", db2.sqlTx)))

	if db.DebugExec || db.Debug {
		log.Printf("%s BEGIN: %s sql.DB: %p", db, &db2, db.sqlDB)
	}

	return &db2, nil
}

// Begin starts a new transaction, defaulting to write mode for backwards compatibility
func (db *DB) Begin() (*DB, error) {
	return db.txBegin(true)
}

// BeginRead starts a new transaction, read-only mode
func (db *DB) BeginRead() (*DB, error) {
	return db.txBegin(false)
}

func (db *DB) Commit() error {
	if db.sqlTx == nil {
		panic("sqlpro.DB.Commit: Unable to call Commit without Transaction.")
	}

	if db.DebugExec || db.Debug {
		log.Printf("%s COMMIT sql.DB: %p", db, db.sqlDB)
	}

	// pflib.Pln("[%p] COMMIT #%d %s", db.sqlDB, db.transID, aurora.Blue(fmt.Sprintf("%p", db.sqlTx)))

	return db.sqlTx.Commit()
}

func (db *DB) Rollback() error {
	if db.sqlTx == nil {
		panic("sqlpro.DB.Rollback: Unable to call Rollback without Transaction.")
	}

	if db.DebugExec || db.Debug {
		log.Printf("%s ROLLBACK", db)
	}

	// debug.PrintStack()
	// pflib.Pln("[%p] ROLLBACK #%d %s", db.sqlDB, db.transID, aurora.Blue(fmt.Sprintf("%p", db.sqlTx)))

	return db.sqlTx.Rollback()
}
