package sqlpro

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"reflect"
	"time"

	"github.com/pkg/errors"
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
		isSlice         bool
		isStruct        bool
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
		isStruct = true
	case reflect.Slice:
		isSlice = true

		var isPointer bool

		if targetV.Type().Elem().Kind() == reflect.Ptr {
			isPointer = true
		}

		// append placeholders to the slice
		for range cols {
			var newEl reflect.Value
			if isPointer {
				newEl = reflect.New(targetV.Type().Elem().Elem())
			} else {
				newEl = reflect.Indirect(reflect.New(targetV.Type().Elem()))
			}
			targetV.Set(reflect.Append(targetV, newEl))
		}
	}

	switch targetV.Interface().(type) {
	case time.Time, *time.Time:
		isStruct = false
	}

	// if target.Kind() == reflect.Ptr {
	// 	log.Printf("Target: %v %s %v %s", target.IsValid(), target.Type(), target.IsNil(), target.Type().Elem().Kind())
	// }

	nullValueByIdx := make(map[int]reflect.Value, 0)

	for idx, col := range cols {

		skip := false

		// logrus.Infof("%v %v %v %v", idx, col, isStruct, isSlice)

		if isStruct {
			finfo, ok := info[col]
			if !ok {
				skip = true
			} else {
				fieldV = targetV.FieldByName(finfo.name)
				if finfo.isJson {
					// log.Printf("Setting field to json: %v idx: %d", finfo.name, idx)
					data[idx] = &NullJson{}
					nullValueByIdx[idx] = fieldV
					continue
				}
			}
		} else if isSlice {
			fieldV = targetV.Index(idx)
		} else {
			if idx == 0 {
				// first column will be mapped
				fieldV = targetV
			} else {
				skip = true
			}
		}

		if skip {
			// column not mapped in struct, we still need to allocate
			data[idx] = &voidScan{}
			continue
		}

		// log.Printf("NIL?: %v %s %T", fieldV.IsValid(), fieldV.Type(), fieldV.Interface())

		// Init Null Scanners for some Pointer Types
		switch fieldV.Interface().(type) { // FIXME: we could use reflect's Type here
		case *json.RawMessage, json.RawMessage:
			data[idx] = &NullRawMessage{}
			nullValueByIdx[idx] = fieldV
		case *string, string:
			data[idx] = &sql.NullString{}
			nullValueByIdx[idx] = fieldV
		case *int64, int64, *int32, int32, *int16, int16, *int8, int8, *int, int,
			uint32, *uint32, uint16, *uint16, uint8, *uint8, uint64, *uint64, uint, *uint:
			data[idx] = &sql.NullInt64{}
			nullValueByIdx[idx] = fieldV
		case *float64, float64, *float32, float32:
			data[idx] = &sql.NullFloat64{}
			nullValueByIdx[idx] = fieldV
		case *bool, bool:
			data[idx] = &sql.NullBool{}
			nullValueByIdx[idx] = fieldV
		case time.Time, *time.Time:
			data[idx] = &NullTime{}
			nullValueByIdx[idx] = fieldV
		default:
			if fieldV.Kind() != reflect.Ptr {
				// Pass a pointer
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
		switch v := data[idx].(type) {
		case *NullJson:
			if (*v).Valid {
				// unmarshal
				newData := reflect.New(fieldV.Type())
				err = json.Unmarshal((*v).Data, newData.Interface())
				if err != nil {
					return errors.Wrapf(err, "Error unmarshalling data: %q", string((*v).Data))
				}
				fieldV.Set(reflect.Indirect(reflect.Value(newData)))
			} else {
				fieldV.Set(reflect.Zero(fieldV.Type()))
			}
			continue
		case *NullRawMessage:
			if (*v).Valid {
				if fieldV.Type().Kind() == reflect.Ptr {
					fieldV.Set(reflect.ValueOf(&(*v).Data))
				} else {
					fieldV.Set(reflect.ValueOf((*v).Data))
				}
			} else {
				fieldV.Set(reflect.Zero(fieldV.Type()))
			}
			continue
		}

		switch v0 := fieldV.Interface().(type) {
		case *string, *int32, *int16, *int8, *int64, *uint64, *uint, *uint16, *uint32, *float64, *int, *bool:
			switch v := data[idx].(type) {
			case *sql.NullBool:
				if (*v).Valid {
					fieldV.Set(reflect.ValueOf(&(*v).Bool))
				} else {
					fieldV.Set(reflect.Zero(fieldV.Type()))
				}
			case *sql.NullString:
				if (*v).Valid {
					fieldV.Set(reflect.ValueOf(&(*v).String))
				} else {
					fieldV.Set(reflect.Zero(fieldV.Type()))
				}
			case *sql.NullInt64:
				if (*v).Valid {
					i64 := (*v).Int64

					switch v0.(type) {
					case *int64:
						fieldV.Set(reflect.ValueOf(&i64))
					case *int32:
						i32 := int32(i64)
						fieldV.Set(reflect.ValueOf(&i32))
					case *int16:
						i16 := int16(i64)
						fieldV.Set(reflect.ValueOf(&i16))
					case *int8:
						i8 := int8(i64)
						fieldV.Set(reflect.ValueOf(&i8))
					case *int:
						i := int(i64)
						fieldV.Set(reflect.ValueOf(&i))
					case *uint64:
						ui64 := uint64(i64)
						fieldV.Set(reflect.ValueOf(&ui64))
					case *uint32:
						ui32 := uint32(i64)
						fieldV.Set(reflect.ValueOf(&ui32))
					case *uint16:
						ui16 := uint16(i64)
						fieldV.Set(reflect.ValueOf(&ui16))
					case *uint8:
						ui8 := uint8(i64)
						fieldV.Set(reflect.ValueOf(&ui8))
					case *uint:
						ui := uint(i64)
						fieldV.Set(reflect.ValueOf(&ui))
					}
				} else {
					fieldV.Set(reflect.Zero(fieldV.Type()))
				}
			case *sql.NullFloat64:
				if (*v).Valid {
					f64 := (*v).Float64
					switch v0.(type) {
					case *float64:
						fieldV.Set(reflect.ValueOf(&f64))
					case *float32:
						f32 := float32(f64)
						fieldV.Set(reflect.ValueOf(&f32))
					}
				} else {
					fieldV.Set(reflect.Zero(fieldV.Type()))
				}
			}
		case string, int64, float64, int, int32, int8, int16:
			switch v := data[idx].(type) {
			case *sql.NullString:
				fieldV.SetString((*v).String)
			case *sql.NullInt64:
				switch v0.(type) {
				case int64, int32, int16, int8, int:
					fieldV.SetInt((*v).Int64)
				}

			case *sql.NullFloat64:
				fieldV.SetFloat((*v).Float64)
			}
		case uint64, uint32, uint16, uint8, uint:
			switch v := data[idx].(type) {
			case *sql.NullInt64:
				fieldV.SetUint(uint64((*v).Int64))
			}
		case bool:
			switch v := data[idx].(type) {
			case *sql.NullBool:
				fieldV.SetBool((*v).Bool)
			}
		case time.Time:
			switch v := data[idx].(type) {
			case *NullTime:
				if (*v).Valid {
					fieldV.Set(reflect.ValueOf(v.Time))
				} else {
					fieldV.Set(reflect.Zero(fieldV.Type()))
				}
			default:
				panic(fmt.Sprintf("sqlpro: unable to read back time.Time into %q", cols[idx]))
			}
		case *time.Time:
			switch v := data[idx].(type) {
			case *NullTime:
				if (*v).Valid {
					fieldV.Set(reflect.ValueOf(&(*v).Time))
				} else {
					fieldV.Set(reflect.Zero(fieldV.Type()))
				}
			default:
				panic(fmt.Sprintf("sqlpro: unable to read back *time.Time into %q", cols[idx]))
			}
		default:
			panic(fmt.Sprintf("sqlpro: unable to read back null into %q: %T", cols[idx], fieldV.Interface()))
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
func Scan(target interface{}, rows *sql.Rows) error {
	var (
		targetValue reflect.Value
		rowMode     bool
		err         error
	)

	if target == nil {
		panic(fmt.Errorf("Scan: target must not be <nil>."))
	}

	v := reflect.ValueOf(target)
	if v.Type().Kind() != reflect.Ptr {
		panic(fmt.Errorf("Scan: non-pointer %v", v.Type()))
	}

	targetValue = v.Elem()
	if !targetValue.CanAddr() {
		panic("Scan: Unable to use unadressable field as target.")
	}

	if targetValue.Type().Kind() != reflect.Slice {
		rowMode = true
	}

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

	if rowMode {
		// If we get here with row mode, it means we have nothing found
		// return an error
		return ErrQueryReturnedZeroRows
	}

	return nil

}
