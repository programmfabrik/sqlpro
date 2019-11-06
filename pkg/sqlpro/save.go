package sqlpro

import (
	"fmt"
	"reflect"
)

// Save saves the given data. It performs an INSERT if the only
// primary key is zero, and and UPDATE if it is not. It panics
// if it the record has no primary key or less than one
func (db *DB) Save(table string, data interface{}) (interface{}, error) {
	rv, structMode, err := checkData(data)
	if err != nil {
		return nil, err
	}

	if structMode {
		return db.saveRow(table, data)
	} else {
		for i := 0; i < rv.Len(); i++ {
			_, err = db.saveRow(table, rv.Index(i).Interface())
			if err != nil {
				return nil, err
			}
		}
	}

	// TODO: Return data here
	return nil, nil
}

func (db *DB) saveRow(table string, data interface{}) (interface{}, error) {
	row := reflect.Indirect(reflect.ValueOf(data))

	values, info, err := db.valuesFromStruct(row.Interface())
	if err != nil {
		return nil, err
	}
	pk := info.onlyPrimaryKey()

	if pk == nil {
		return nil, fmt.Errorf("Save needs a struct with exactly one 'pk' field.")
	}

	pk_value, ok := values[pk.dbName]
	if !ok || isZero(pk_value) {
		return db.Insert(table, data)
	} else {
		return db.Update(table, data)
	}
}
