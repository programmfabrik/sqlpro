package sqlpro

// Begin starts a new connection, this panics if
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
	db2.DB = db2.sqlTx

	return &db2, nil
}

func (db *DB) Commit() error {
	if db.sqlTx == nil {
		panic("sqlpro.DB.Commit: Unable to call Commit without Transaction.")
	}
	return db.sqlTx.Commit()
}

func (db *DB) Rollback() error {
	if db.sqlTx == nil {
		panic("sqlpro.DB.Rollback: Unable to call Rollback without Transaction.")
	}
	return db.sqlTx.Rollback()
}
