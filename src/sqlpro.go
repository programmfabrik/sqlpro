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

// SelectOneRow runs query and fills the row.
func (db *DB) SelectRow(row interface{}, query string, args ...interface{}) error {
	var (
		rows *sql.Rows
		err  error
		data []interface{} // data is used for scanning
		cols []string      // columns for the query

	)

	v := reflect.ValueOf(row)
	if v.Kind() != reflect.Ptr {
		return fmt.Errorf("non-pointer %v", v.Type())
	}

	targetV := v.Elem()
	if targetV.Kind() != reflect.Struct {
		return fmt.Errorf("struct expected: %s", targetV.Kind())
	}

	rows, err = db.DB.Query(query, args...)
	if err != nil {
		return err
	}

	cols, err = rows.Columns()
	if err != nil {
		return err
	}

	data = make([]interface{}, len(cols))

	fields := fieldMap(reflect.ValueOf(targetV.Interface()).Type())
	nullValueByIdx := make(map[int]reflect.Value, 0)

	for idx, col := range cols {
		finfo, ok := fields[col]
		if !ok {
			// column not mapped in struct, we sill need to allocate
			//
			data[idx] = &voidScan{}
			continue
		}

		fieldV := targetV.FieldByName(finfo.name)

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

	defer rows.Close()
	for rows.Next() {
		err = rows.Scan(data...)
		if err != nil {
			return err
		}
	}

	// Read back data from Null scanners which we used above
	for idx, fieldV := range nullValueByIdx {
		switch fieldV.Interface().(type) {
		case *string, *int64:
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
