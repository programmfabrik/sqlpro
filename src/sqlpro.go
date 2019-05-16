package sqlpro

import (
	"database/sql"
	"fmt"
	"reflect"
)

type DB struct {
	DB *sql.DB
}

// NewSqlPro returns a wrapped database handle providing
// access to the sql pro functions.
func NewSqlPro(dbWrap *sql.DB) *DB {
	var (
		db *DB
	)
	db = new(DB)
	db.DB = dbWrap
	return db
}

type fieldInfo struct {
	structField reflect.StructField
	name        string
}

type voidScan struct{}

func (vs *voidScan) Scan(interface{}) error {
	// Don't do anything
	return nil
}

// fieldMap returns
func fieldMap(t reflect.Type) map[string]fieldInfo {
	m := make(map[string]fieldInfo, 0)

	// log.Printf("name: %s %d", t, t.NumField())
	for i := 0; i < t.NumField(); i++ {
		field := t.Field(i)
		if field.PkgPath != "" {
			// unexported field
			continue
		}
		dbName := field.Tag.Get("db")
		if dbName == "-" {
			continue
		}
		if dbName == "" {
			dbName = field.Name
		}
		m[dbName] = fieldInfo{structField: field, name: field.Name}
	}
	return m
}

func (db *DB) scanRow(target reflect.Value, rows *sql.Rows) error {
	var (
		err             error
		cols            []string
		data            []interface{}
		targetV, fieldV reflect.Value
		structFields    map[string]fieldInfo
	)

	cols, err = rows.Columns()
	if err != nil {
		return err
	}

	data = make([]interface{}, len(cols))

	if target.Kind() == reflect.Ptr {
		if target.IsNil() {
			// nil pointer
			// if target.Type().Elem().Kind() == reflect.Struct {
			target.Set(reflect.New(target.Type().Elem()))
			// }
		}
		// log.Printf("Kind: %v", target.Elem().Kind())
		if target.Elem().Kind() == reflect.Struct {
			targetV = target.Elem()
		} else {
			targetV = target
		}
	} else {
		targetV = target
	}

	switch targetV.Kind() {
	case reflect.Struct:
		structFields = fieldMap(reflect.ValueOf(targetV.Interface()).Type())
	case reflect.Slice:
		return fmt.Errorf("Slice target not supported.")
	}

	// if target.Kind() == reflect.Ptr {
	// 	log.Printf("Target: %v %s %v %s", target.IsValid(), target.Type(), target.IsNil(), target.Type().Elem().Kind())
	// }

	nullValueByIdx := make(map[int]reflect.Value, 0)

	for idx, col := range cols {

		skip := false

		if structFields != nil {
			finfo, ok := structFields[col]
			if !ok {
				skip = true
			} else {
				fieldV = targetV.FieldByName(finfo.name)
			}
		} else {
			if idx == 0 {
				// first column will be mapped
				fieldV = targetV
			} else {
				skip = true
			}
		}

		if skip {
			// column not mapped in struct, we sill need to allocate
			data[idx] = &voidScan{}
			continue
		}

		// log.Printf("NIL?: %v %s %T", fieldV.IsValid(), fieldV.Type(), fieldV.Interface())

		// Init Null Scanners for some Pointer Types
		switch fieldV.Interface().(type) {
		case *string, string:
			data[idx] = &sql.NullString{}
			nullValueByIdx[idx] = fieldV
		case *int64, int64:
			data[idx] = &sql.NullInt64{}
			nullValueByIdx[idx] = fieldV
		case *float64, float64:
			data[idx] = &sql.NullFloat64{}
			nullValueByIdx[idx] = fieldV
		default:
			if fieldV.Kind() != reflect.Ptr {
				// Pass the pointer to the value
				data[idx] = fieldV.Addr().Interface()
			} else {
				if fieldV.IsNil() {
					fieldV.Set(reflect.New(fieldV.Type().Elem()))
				}
				data[idx] = fieldV.Interface()
			}
		}
	}

	err = rows.Scan(data...)
	if err != nil {
		return err
	}

	// Read back data from Null scanners which we used above
	for idx, fieldV := range nullValueByIdx {
		switch fieldV.Interface().(type) {
		case *string, *int64, *float64:
			switch v := data[idx].(type) {
			case *sql.NullString:
				if (*v).Valid {
					fieldV.Set(reflect.ValueOf(&(*v).String))
				} else {
					fieldV.Set(reflect.Zero(fieldV.Type()))
				}
			case *sql.NullInt64:
				if (*v).Valid {
					fieldV.Set(reflect.ValueOf(&(*v).Int64))
				} else {
					fieldV.Set(reflect.Zero(fieldV.Type()))
				}
			case *sql.NullFloat64:
				if (*v).Valid {
					fieldV.Set(reflect.ValueOf(&(*v).Float64))
				} else {
					fieldV.Set(reflect.Zero(fieldV.Type()))
				}
			}
		case string, int64, float64:
			switch v := data[idx].(type) {
			case *sql.NullString:
				fieldV.SetString((*v).String)
			case *sql.NullInt64:
				fieldV.SetInt((*v).Int64)
			case *sql.NullFloat64:
				fieldV.SetFloat((*v).Float64)
			}
		}
	}
	return nil
}

// SelectOneRow runs query and fills the row.
func (db *DB) Select(target interface{}, query string, args ...interface{}) error {
	var (
		rows        *sql.Rows
		err         error
		rowMode     bool
		targetValue reflect.Value
	)

	v := reflect.ValueOf(target)
	if v.Type().Kind() != reflect.Ptr {
		return fmt.Errorf("non-pointer %v", v.Type())
	}

	targetValue = v.Elem()
	if targetValue.Type().Kind() != reflect.Slice {
		rowMode = true
	}

	// log.Printf("RowMode: %s %v", targetValue.Type().Kind(), rowMode)

	rows, err = db.DB.Query(query, args...)
	if err != nil {
		return err
	}

	defer rows.Close()
	for rows.Next() {
		if rowMode {
			err = db.scanRow(targetValue, rows)
			if err != nil {
				return err
			}
			// Only one row in row mode
			return nil
		}

		// slice mode

		// create an item suitable for appending to the slice
		rowValues := reflect.MakeSlice(targetValue.Type(), 1, 1)
		rowValue := rowValues.Index(0)

		err = db.scanRow(rowValue, rows)
		if err != nil {
			return err
		}

		targetValue.Set(reflect.Append(targetValue, rowValue))
	}

	return nil
}
