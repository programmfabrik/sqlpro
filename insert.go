package sqlpro

import (
	"fmt"
	"reflect"
	"strings"
)

// Insert takes a table name and a struct and inserts
// the record in the DB.
// The given data needs to be:
//
// *[]*strcut
// *[]struct
// []*struct
// []struct
// struct
// *struct
//
// sqlpro will executes one INSERT statement per row.
// result.LastInsertId will be used to set the first primary
// key column.

func (db *DB) Insert(table string, data interface{}) (interface{}, error) {
	value, structMode, err := checkData(data)
	if err != nil {
		return nil, err
	}

	if !structMode {
		for rowNumber := 0; rowNumber < value.Len(); rowNumber++ {
			// TODO: How to handle an error in the middle of the data? Rollback or Insert everything possible?
			err := insertAndSetPrimaryKey(table, reflect.Indirect(value.Index(rowNumber)))
			if err != nil {
				return nil, err
			}
		}
	} else {
		err := insertAndSetPrimaryKey(table, value)
		if err != nil {
			return nil, err
		}
	}

	// TODO: Return the data with primary keys
	return nil, nil
}

func insertAndSetPrimaryKey(table string, data reflect.Value) error {
	insert_id, structInfo, err := db.insertStruct(table, data.Interface())
	if err != nil {
		return err
	}

	pk := structInfo.onlyPrimaryKey()
	if pk != nil && pk.structField.Type.Kind() == reflect.Int64 {
		setPrimaryKey(data.FieldByName(pk.name), insert_id)
	}
	return nil
}

func (db *DB) insertStruct(table string, row interface{}) (int64, structInfo, error) {
	values, info, err := db.valuesFromStruct(row)
	if err != nil {
		return 0, nil, err
	}

	sql, args, err := db.insertClauseFromValues(table, values, info)
	if err != nil {
		return 0, nil, err
	}

	if db.UseReturningForLastId {
		pk := info.onlyPrimaryKey()
		if pk != nil && pk.structField.Type.Kind() == reflect.Int64 {
			sql = sql + " RETURNING " + db.Esc(pk.dbName)

			var insert_id int64 = 0
			err := db.Query(&insert_id, sql, args...)
			if err != nil {
				return 0, nil, err
			}

			return insert_id, info, nil
		}
	}

	insert_id, err := db.exec(1, sql, args...)
	if err != nil {
		return 0, nil, err
	}

	return insert_id, info, nil
}

func (db *DB) insertClauseFromValues(table string, values map[string]interface{}, info structInfo) (string, []interface{}, error) {
	cols := make([]string, 0, len(values))
	vs := make([]string, 0, len(values))
	args := make([]interface{}, 0, len(values))

	for col, value := range values {
		cols = append(cols, db.Esc(col))
		vs = append(vs, "?")
		args = append(args, db.nullValue(value, info[col]))
	}
	return fmt.Sprintf("INSERT INTO %s (%s) VALUES(%s)",
		db.Esc(table),
		strings.Join(cols, ","),
		strings.Join(vs, ","),
	), args, nil
}
