package sqlpro

import (
	"fmt"
	"reflect"

	"github.com/lib/pq"
	"golang.org/x/xerrors"
)

func (db *DB) InsertBulkCopyIn(table string, data interface{}) error {
	var (
		rv         reflect.Value
		structMode bool
		err        error
	)

	rv, structMode, err = checkData(data)
	if err != nil {
		return err
	}

	if structMode {
		return fmt.Errorf("InsertBulk: Need Slice to insert bulk.")
	}

	key_map := make(map[string]*fieldInfo, 0)
	rows := make([]map[string]interface{}, 0)

	if rv.Len() == 0 {
		return nil
	}

	for i := 0; i < rv.Len(); i++ {
		row := reflect.Indirect(rv.Index(i)).Interface()

		values, structInfo, err := db.valuesFromStruct(row)

		if err != nil {
			return xerrors.Errorf("sqlpro.InsertBulk error: %w", err)
		}

		rows = append(rows, values)
		for key := range values {
			key_map[key] = structInfo[key]
		}
	}

	txn, err := db.sqlDB.Begin()
	if err != nil {
		return sqlError(err, "BEGIN TRANSACTION", []interface{}{})
	}

	keys := make([]string, 0, len(key_map))
	for key := range key_map {
		keys = append(keys, key)
	}

	stmt, err := txn.Prepare(pq.CopyIn(table, keys...))
	if err != nil {
		return sqlError(err, "Prepare", []interface{}{})
	}

	for _, row := range rows {
		values := make([]interface{}, 0, len(key_map))
		for _, key := range keys {
			values = append(values, row[key])
		}
		_, err = stmt.Exec(values...)
		if err != nil {
			return sqlError(err, "Exec", values)
		}
	}

	_, err = stmt.Exec()
	if err != nil {
		return sqlError(err, "Exec DONE", []interface{}{})
	}

	err = txn.Commit()
	if err != nil {
		return sqlError(err, "Commit DONE", []interface{}{})
	}

	return nil
}
