package sqlpro

import (
	"encoding/json"
	"fmt"
	"reflect"

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
