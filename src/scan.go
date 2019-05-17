package sqlpro

import (
	"database/sql"
	"fmt"
	"reflect"
)

type voidScan struct{}

func (vs *voidScan) Scan(interface{}) error {
	// Don't do anything
	return nil
}

// scanRow scans one row into the given target
func scanRow(target reflect.Value, rows *sql.Rows) error {
	var (
		err             error
		cols            []string
		data            []interface{}
		targetV, fieldV reflect.Value
		info            structInfo
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
		info = getStructInfo(reflect.ValueOf(targetV.Interface()).Type())
	case reflect.Slice:
		return fmt.Errorf("Slice target not supported.")
	}

	// if target.Kind() == reflect.Ptr {
	// 	log.Printf("Target: %v %s %v %s", target.IsValid(), target.Type(), target.IsNil(), target.Type().Elem().Kind())
	// }

	nullValueByIdx := make(map[int]reflect.Value, 0)

	for idx, col := range cols {

		skip := false

		if info != nil {
			finfo, ok := info[col]
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

// Scan reads data from the given rows into the target.
//
// *int64, *string, etc: First column of first row
// *struct: First row
// []int64, []*int64, []string, []*string: First column, all rows
// []struct, []*struct: All columns, all rows
//
// The mapping into structs is done by analyzing the struct's tag names
// and using the given "db" key for the mapping. The mapping works on
// exported fields only. Use "-" as mapping name to ignore the field.
//
func Scan(target interface{}, rows *sql.Rows) error {
	var (
		targetValue reflect.Value
		rowMode     bool
		err         error
	)

	v := reflect.ValueOf(target)
	if v.Type().Kind() != reflect.Ptr {
		return fmt.Errorf("non-pointer %v", v.Type())
	}

	targetValue = v.Elem()
	if targetValue.Type().Kind() != reflect.Slice {
		rowMode = true
	}

	defer rows.Close()
	for rows.Next() {
		if rowMode {
			err = scanRow(targetValue, rows)
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

		err = scanRow(rowValue, rows)
		if err != nil {
			return err
		}

		targetValue.Set(reflect.Append(targetValue, rowValue))
	}

	return nil

}
