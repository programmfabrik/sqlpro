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
		structMode = true
	default:
		return err()
	}

	return rv, structMode, nil
}

func setPrimaryKey(rv reflect.Value, id int64) {
	if !rv.CanAddr() {
		return
	}
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
