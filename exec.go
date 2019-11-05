package sqlpro

import (
	"encoding/json"
	"fmt"
	"reflect"
	"strings"

	"github.com/lib/pq"
	"golang.org/x/xerrors"
)

// checkData checks that the given data is either one of:
//
// *[]*strcut
// *[]struct
// []*struct
// []struct
// *struct
//
// For structs the function returns true, nil, for slices false, nil

func checkData(data interface{}) (reflect.Value, bool, error) {
	var (
		rv         reflect.Value
		structMode bool
	)

	err := func() (reflect.Value, bool, error) {
		return rv, false, fmt.Errorf("Insert/Update needs a struct or slice of structs.")
	}

	rv = reflect.Indirect(reflect.ValueOf(data))

	switch rv.Type().Kind() {
	case reflect.Slice:
		switch rv.Type().Elem().Kind() {
		case reflect.Ptr:
			if rv.Type().Elem().Elem().Kind() != reflect.Struct {
				return err()
			}
		case reflect.Interface, reflect.Struct:
		default:
			return rv, false, fmt.Errorf("Insert/Update needs a slice of structs. Have: %s", rv.Type().Elem().Kind())
		}
	case reflect.Struct:
		if !rv.CanAddr() {
			return err()
		}
		structMode = true
	default:
		return err()
	}

	return rv, structMode, nil
}

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

func (db *DB) Insert(table string, data interface{}) error {
	var (
		rv         reflect.Value
		structMode bool
		err        error
	)

	rv, structMode, err = checkData(data)
	if err != nil {
		return err
	}

	if !structMode {
		for i := 0; i < rv.Len(); i++ {
			row := reflect.Indirect(rv.Index(i))
			insert_id, structInfo, err := db.insertStruct(table, row.Interface())
			if err != nil {
				return err
			}
			pk := structInfo.onlyPrimaryKey()
			if pk != nil && pk.structField.Type.Kind() == reflect.Int64 {
				setPrimaryKey(row.FieldByName(pk.name), insert_id)
			}
		}
	} else {
		insert_id, structInfo, err := db.insertStruct(table, rv.Interface())
		if err != nil {
			return err
		}
		pk := structInfo.onlyPrimaryKey()
		// log.Printf("PK: %d", insert_id)
		if pk != nil && pk.structField.Type.Kind() == reflect.Int64 {
			setPrimaryKey(rv.FieldByName(pk.name), insert_id)
		}
	}

	// data
	return nil
}

func setPrimaryKey(rv reflect.Value, id int64) {
	switch rv.Type().Kind() {
	case reflect.Int64:
		rv.SetInt(id)
	case reflect.Uint64:
		rv.SetUint(uint64(id))
	default:
		err := fmt.Errorf("Unknown type to set primary key: %s", rv.Type())
		panic(err)
	}
}

// InsertBulk takes a table name and a slice of struct and inserts
// the record in the DB with one Exec.
// The given data needs to be:
//
// *[]*strcut
// *[]struct
// []*struct
// []struct
//
// sqlpro will executes one INSERT statement per call.
func (db *DB) InsertBulk(table string, data interface{}) error {
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

	insert := strings.Builder{} // make([]string, 0)
	keys := make([]string, 0, len(key_map))

	insert.WriteString("INSERT INTO ")
	insert.WriteString(db.Esc(table))
	insert.WriteString(" (")

	idx := 0
	for key := range key_map {
		if idx > 0 {
			insert.WriteRune(',')
		}
		insert.WriteString(db.Esc(key))
		keys = append(keys, key)
		idx++
	}

	insert.WriteString(") VALUES ")

	for idx, row := range rows {
		if idx > 0 {
			insert.WriteRune(',')
		}
		insert.WriteRune('(')
		for idx2, key := range keys {
			if idx2 > 0 {
				insert.WriteRune(',')
			}
			insert.WriteString(db.EscValueForInsert(row[key], key_map[key]))
		}
		insert.WriteRune(')')
	}

	_, err = db.DB.Exec(insert.String())
	if err != nil {
		return sqlError(err, insert.String(), []interface{}{})
	}

	return nil
}

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
			// log.Printf("Returning ID: %d", insert_id)
			return insert_id, info, nil
		}
	}

	// log.Printf("SQL: %s Debug: %v", sql, db.Debug)
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

	update := strings.Builder{}
	where := strings.Builder{}

	update.WriteString("UPDATE ")
	update.WriteString(db.Esc(table))
	update.WriteString(" SET ")

	where.WriteString(" WHERE ")

	idx := 0
	for key, value := range values {
		if structInfo.primaryKey(key) {
			// skip primary keys for update
			pk_value = db.nullValue(value, structInfo[key])
			if pk_value == nil {
				return "", args, fmt.Errorf("Unable to build UPDATE clause with <nil> key: %s", key)
			}
			where.WriteString(db.Esc(key))
			where.WriteString("=")
			where.WriteRune(db.PlaceholderValue)
			valid = true
		} else {
			if idx > 0 {
				update.WriteString(",")
			}
			update.WriteString(db.Esc(key))
			update.WriteString("=")
			update.WriteRune(db.PlaceholderValue)
			args = append(args, db.nullValue(value, structInfo[key]))
			idx++
		}
	}

	if !valid {
		return "", args, fmt.Errorf("Unable to build UPDATE clause, at least one key needed.")
	}

	args = append(args, pk_value)

	// Add where clause
	return update.String() + where.String(), args, nil
}

// Update updates the given struct or slice of structs
// The WHERE clause is put together from the "pk" columns.
// If not all "pk" columns have non empty values, Update returns
// an error.
func (db *DB) Update(table string, data interface{}) error {
	var (
		rv         reflect.Value
		structMode bool
		err        error
		update     string
		args       []interface{}
	)

	rv, structMode, err = checkData(data)
	if err != nil {
		return err
	}

	if structMode {
		update, args, err = db.updateClauseFromRow(table, rv.Interface())
		if err != nil {
			return err
		}
		_, err = db.exec(1, update, args...)
		if err != nil {
			return err
		}
	} else {
		for i := 0; i < rv.Len(); i++ {
			row := reflect.Indirect(rv.Index(i))
			update, args, err = db.updateClauseFromRow(table, row.Interface())
			if err != nil {
				return err
			}
			_, err = db.exec(1, update, args...)
			if err != nil {
				return err
			}
		}
	}

	return nil
}

// Save saves the given data. It performs an INSERT if the only
// primary key is zero, and and UPDATE if it is not. It panics
// if it the record has no primary key or less than one
func (db *DB) Save(table string, data interface{}) error {

	rv, structMode, err := checkData(data)
	if err != nil {
		return err
	}

	if structMode {
		return db.saveRow(table, data)
	} else {
		for i := 0; i < rv.Len(); i++ {

			if rv.Index(0).Elem().Type().Kind() == reflect.Struct {

				copy := reflect.New(rv.Index(0).Elem().Type())
				copy.Elem().Set(rv.Index(i).Elem())

				err = db.saveRow(table, copy.Interface())
				if err != nil {
					return err
				}

				// addressability continuation hack: assign the copy to index i so that the changes will persist
				rv.Index(i).Set(copy)
			} else {

				err = db.saveRow(table, rv.Index(i).Interface())
				if err != nil {
					return err
				}
			}

		}
	}

	return nil
}

func (db *DB) saveRow(table string, data interface{}) error {
	row := reflect.Indirect(reflect.ValueOf(data))

	values, info, err := db.valuesFromStruct(row.Interface())
	if err != nil {
		return err
	}
	pk := info.onlyPrimaryKey()

	if pk == nil {
		return fmt.Errorf("Save needs a struct with exactly one 'pk' field.")
	}

	pk_value, ok := values[pk.dbName]
	if !ok || isZero(pk_value) {
		return db.Insert(table, data)
	} else {
		return db.Update(table, data)
	}

}

// valuesFromStruct returns the relevant values
// from struct, as map
func (db *DB) valuesFromStruct(data interface{}) (map[string]interface{}, structInfo, error) {
	var (
		info   structInfo
		values map[string]interface{}
		dataV  reflect.Value
		err    error
	)

	values = make(map[string]interface{}, 0)
	dataV = reflect.ValueOf(data)

	info = getStructInfo(dataV.Type())

	for _, fieldInfo := range info {
		dataF := dataV.FieldByName(fieldInfo.name)

		actualData := dataF.Interface()
		isZero := isZero(actualData)

		if isZero && fieldInfo.omitEmpty {
			continue
		}

		if fieldInfo.readOnly {
			continue
		}

		if fieldInfo.isJson {
			if isZero {
				if fieldInfo.ptr {
					actualData = reflect.Zero(fieldInfo.structField.Type).Interface()
				} else {
					actualData = ""
				}
			} else {
				actualData, err = json.Marshal(actualData)
			}
			if err != nil {
				return nil, nil, xerrors.Errorf("Unable to marshal as data as json: %s", err)
			}
		}

		values[fieldInfo.dbName] = actualData
		// log.Printf("Name: %s Value: %v %v", fieldInfo.name, dataF.Interface(), isZero)
	}
	return values, info, nil
}

// isZero returns true if given "x" equals Go's empty value.
func isZero(x interface{}) bool {
	if x == nil {
		return true
	}
	return reflect.DeepEqual(x, reflect.Zero(reflect.TypeOf(x)).Interface())
}
