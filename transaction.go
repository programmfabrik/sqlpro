package sqlpro

import (
	"log"
)

var transID = 0

// Begin starts a new transaction, this panics if
// the wrapper was not initialized using "Open"
func (db *DB) Begin() (*DB, error) {
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

	db2.sqlTx, err = db.sqlDB.Begin()
	if err != nil {
		return nil, err
	}
	db2.db = db2.sqlTx

	// pflib.Pln("[%p] BEFORE BEGIN #%d %s", db.sqlDB, db2.transID, aurora.Blue(fmt.Sprintf("%p", db2.sqlTx)))
	// debug.PrintStack()

	db2.lock()

	db2.transID = transID
	transID++

	// pflib.Pln("[%p] BEGIN #%d %s", db.sqlDB, db2.transID, aurora.Blue(fmt.Sprintf("%p", db2.sqlTx)))

	if db.DebugExec || db.Debug {
		log.Printf("%s BEGIN: %s sql.DB: %p", db, &db2, db.sqlDB)
	}

	return &db2, nil
}

func (db *DB) Commit() error {
	if db.sqlTx == nil {
		panic("sqlpro.DB.Commit: Unable to call Commit without Transaction.")
	}

	if db.DebugExec || db.Debug {
		log.Printf("%s COMMIT sql.DB: %p", db, db.sqlDB)
	}

	// pflib.Pln("[%p] COMMIT #%d %s", db.sqlDB, db.transID, aurora.Blue(fmt.Sprintf("%p", db.sqlTx)))

	defer db.unlock()
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

	defer db.unlock()
	return db.sqlTx.Rollback()
}
