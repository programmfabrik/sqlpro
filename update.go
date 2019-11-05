package sqlpro

import (
	"fmt"
	"reflect"
)

// Update updates the given struct or slice of structs
// The WHERE clause is put together from the "pk" columns.
// If not all "pk" columns have non empty values, Update returns
// an error.
func (db *DB) Update(table string, data interface{}) (interface{}, error) {
	value, structMode, err := checkData(data)
	if err != nil {
		return nil, err
	}

	if structMode {
		if err = db.update(table, value); err != nil {
			return nil, err
		}
	} else {
		for i := 0; i < value.Len(); i++ {
			// TODO: How to handle an error in the middle of the data? Rollback or Update everything possible?
			if err = db.update(table, reflect.Indirect(value.Index(i))); err != nil {
				return nil, err
			}
		}
	}

	// TODO: Return the data
	return nil, nil
}

func (db *DB) update(table string, data reflect.Value) error {
	update, args, err := db.updateClauseFromRow(table, data.Interface())
	if err != nil {
		return err
	}

	_, err = db.exec(1, update, args...)

	return err
}

func (db *DB) updateClauseFromRow(table string, row interface{}) (string, []interface{}, error) {

	var (
		valid    bool
		args     []interface{}
		pk_value interface{}
	)

	values, structInfo, err := db.valuesFromStruct(row)
	if err != nil {
		return "", nil, err
	}

	update := "UPDATE " + db.Esc(table) + " SET "
	where := " WHERE "

	idx := 0
	for key, value := range values {
		if structInfo.primaryKey(key) {
			// skip primary keys for update
			pk_value = db.nullValue(value, structInfo[key])
			if pk_value == nil {
				return "", args, fmt.Errorf("Unable to build UPDATE clause with <nil> key: %s", key)
			}
			where += db.Esc(key) + "=" + string(db.PlaceholderValue)
			valid = true
		} else {
			if idx > 0 {
				update += ","
			}
			update += db.Esc(key) + "=" + string(db.PlaceholderValue)
			args = append(args, db.nullValue(value, structInfo[key]))
			idx++
		}
	}

	if !valid {
		return "", args, fmt.Errorf("Unable to build UPDATE clause, at least one key needed.")
	}

	args = append(args, pk_value)

	return update + where, args, nil
}
