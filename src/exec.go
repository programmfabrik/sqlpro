package sqlpro

import (
	"database/sql"
	"fmt"
	"log"
	"reflect"
	"strings"
)

// Insert takes a table name and a struct and inserts
// the record in the DB.
// The given data needs to be:
// *[]*strcut
// *[]struct
// []*struct
// []struct

func (db *DB) Insert(table string, data interface{}) error {
	var (
		rv         reflect.Value
		structMode bool
		err        error
	)

	rv = reflect.Indirect(reflect.ValueOf(data))

	switch rv.Type().Kind() {
	case reflect.Slice:
		switch rv.Type().Elem().Kind() {
		case reflect.Ptr:
			if rv.Type().Elem().Elem().Kind() != reflect.Struct {
				return fmt.Errorf("Insert needs a slice of structs.")
			}
		case reflect.Struct:
			structMode = true
		default:
			return fmt.Errorf("Insert needs a slice of structs.")
		}
	case reflect.Struct:
		structMode = true
	default:
		return fmt.Errorf("Insert needs a slice or struct.")
	}

	if !structMode {
		for i := 0; i < rv.Len(); i++ {
			_, err = db.insertStruct(table, reflect.Indirect(rv.Index(i)).Interface())
			if err != nil {
				return err
			}
		}
	} else {
		_, err = db.insertStruct(table, rv.Interface())
		if err != nil {
			return err
		}
	}

	// data
	return nil
}

func (db *DB) insertStruct(table string, row interface{}) (sql.Result, error) {
	values := db.valuesFromStruct(row)
	sql, args := db.insertClauseFromValues(table, values)
	log.Printf("SQL: %s ARGS: %v", sql, args)
	return db.DB.Exec(sql, args...)
}

func (db *DB) insertClauseFromValues(table string, values map[string]interface{}) (string, []interface{}) {
	cols := make([]string, 0, len(values))
	qm := make([]string, 0, len(values))
	args := make([]interface{}, 0, len(values))

	for col, value := range values {
		cols = append(cols, db.Esc(col))
		qm = append(qm, "?")
		args = append(args, value)
	}
	return fmt.Sprintf("INSERT INTO %s (%s) VALUES(%s)",
		db.Esc(table),
		strings.Join(cols, ","),
		strings.Join(qm, ","),
	), args
}

// valuesFromStruct returns the relevant values
// from struct, as map
func (db *DB) valuesFromStruct(data interface{}) map[string]interface{} {
	var (
		info   structInfo
		values map[string]interface{}
		dataV  reflect.Value
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
		values[fieldInfo.dbName] = actualData
		// log.Printf("Name: %s Value: %v %v", fieldInfo.name, dataF.Interface(), isZero)
	}
	return values
}

func isZero(x interface{}) bool {
	return reflect.DeepEqual(x, reflect.Zero(reflect.TypeOf(x)).Interface())
}
