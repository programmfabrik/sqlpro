package sqlpro

import (
	"database/sql/driver"
	"encoding/json"
	"fmt"
	"reflect"
	"strconv"
	"strings"
	"time"

	"golang.org/x/xerrors"
)

// structInfo is a map to fieldInfo by db_name
type structInfo map[string]*fieldInfo

func (si structInfo) hasDbName(db_name string) bool {
	_, ok := si[db_name]
	return ok
}

func (si structInfo) primaryKey(db_name string) bool {
	fieldInfo, ok := si[db_name]
	if !ok {
		panic(fmt.Sprintf("isPrimaryKey: db_name %s not found.", db_name))
	}
	return fieldInfo.primaryKey
}

func (si structInfo) onlyPrimaryKey() *fieldInfo {
	var (
		fi *fieldInfo
	)

	for _, info := range si {
		if info.primaryKey {
			if fi != nil {
				// more than one
				return nil
			}
			fi = info
		}
	}

	return fi
}

type NullTime struct {
	Time  *time.Time
	Valid bool
}

// Scan implements the Scanner interface.
func (ni *NullTime) Scan(value interface{}) error {
	// log.Printf("Scan %T %s", value, value)
	if value == nil {
		ni.Time, ni.Valid = nil, false
		return nil
	}
	switch v := value.(type) {
	case time.Time:
		ni.Time = &v
		ni.Valid = true
	default:
		return fmt.Errorf("Unable to scan time: %T %s", value, value)
	}
	// pretty.Println(ni)
	return nil

}

type NullJson struct {
	Data  []byte
	Valid bool
}

func (nj *NullJson) Scan(value interface{}) error {
	switch v := value.(type) {
	case nil:
		return nil
	case []byte:
		if len(v) == 0 {
			return nil
		}
		nj.Data = v
		nj.Valid = true
		return nil
	case string:
		if len(v) == 0 {
			return nil
		}
		nj.Data = []byte(v)
		nj.Valid = true
		return nil
	default:
		return xerrors.Errorf("sqlpro.NullBytes.Scan: Unable to scan type %T", value)
	}
}

type NullRawMessage struct {
	Data  json.RawMessage
	Valid bool
}

func (nj *NullRawMessage) Scan(value interface{}) error {
	switch v := value.(type) {
	case nil:
		return nil
	case []byte:
		if len(v) == 0 {
			return nil
		}
		nj.Data = v
		nj.Valid = true
		return nil
	default:
		return xerrors.Errorf("sqlpro.NullBytes.Scan: Unable to Scan type %T", value)
	}
}

type fieldInfo struct {
	structField reflect.StructField
	name        string
	dbName      string
	omitEmpty   bool
	primaryKey  bool
	null        bool
	notNull     bool
	isJson      bool
	emptyValue  string
	ptr         bool // set true if the field is a pointer
}

// allowNull returns true if the given can store "null" values
func (fi *fieldInfo) allowNull() bool {
	if fi.ptr {
		if fi.notNull {
			return false
		}
		return true
	}
	if fi.null {
		return true
	}
	return false
}

// getStructInfo returns a per dbName to fieldInfo map
func getStructInfo(t reflect.Type) structInfo {
	si := make(structInfo, 0)

	// log.Printf("name: %s %d", t, t.NumField())
	for i := 0; i < t.NumField(); i++ {
		field := t.Field(i)

		dbTag := field.Tag.Get("db")
		if dbTag == "" {
			// ignore field
			continue
		}

		if field.PkgPath != "" {
			// unexported field
			panic(fmt.Errorf("getStructInfo: Unable to use unexported field for sqlpro: %s", field.Name))
		}

		path := strings.Split(dbTag, ",")

		info := fieldInfo{
			dbName:      path[0],
			structField: field,
			name:        field.Name,
			omitEmpty:   false,
			primaryKey:  false,
		}

		if info.dbName == "-" {
			continue
		}

		switch field.Type.Kind() {
		case reflect.Ptr:
			info.ptr = true
			info.emptyValue = "null"
		case reflect.String:
			info.emptyValue = "''"
		case reflect.Int:
			info.emptyValue = "0"
		default:
			info.emptyValue = "''"
		}

		if info.dbName == "" {
			info.dbName = field.Name
		}

		for idx, p := range path {
			if idx == 0 {
				continue
			}
			switch p {
			case "pk":
				info.primaryKey = true
			case "omitempty":
				info.omitEmpty = true
			case "null":
				info.null = true
			case "notnull":
				info.notNull = true
			case "json":
				info.isJson = true
			default:
				// ignore unrecognized
			}
		}

		if info.allowNull() && info.emptyValue == "null" {
			info.emptyValue = "''"
		}

		si[info.dbName] = &info
	}
	return si
}

// replaceArgs rewrites the string sqlS to embed the slice args given
// it returns the new placeholder string and the reduced list of arguments.
func (db *DB) replaceArgs(sqlS string, args ...interface{}) (string, []interface{}, error) {
	var (
		nthArg, lenRunes   int
		newArgs            []interface{}
		sb                 strings.Builder
		runes              []rune
		currRune, nextRune rune
	)

	// pretty.Println(args)

	sb = strings.Builder{}
	nthArg = 0

	runes = []rune(sqlS)
	lenRunes = len(runes)

	for i := 0; i < lenRunes; i++ {
		currRune = runes[i]

		if i+1 < lenRunes {
			nextRune = runes[i+1]
		} else {
			nextRune = 0
		}

		if currRune != db.PlaceholderKey && currRune != db.PlaceholderValue {
			sb.WriteRune(currRune)
			continue
		}

		if (currRune == db.PlaceholderValue && nextRune == db.PlaceholderValue) ||
			(currRune == db.PlaceholderKey && nextRune == db.PlaceholderKey) {
			sb.WriteRune(currRune)
			i++
			continue
		}

		// log.Printf("%d curr: %s next: %s", i, string(currRune), string(nextRune))

		if nthArg >= len(args) {
			return "", nil, fmt.Errorf("replaceArgs: Expecting #%d arg. Got: %d args.", (nthArg + 1), len(args))
		}

		arg := args[nthArg]
		nthArg++

		if currRune == db.PlaceholderKey {
			switch v := arg.(type) {
			case *string:
				sb.WriteString(db.Esc(*v))
			case string:
				sb.WriteString(db.Esc(v))
			default:
				return "", nil, fmt.Errorf("replaceArgs: Unable to replace %s with type %T, need *string or string.", string(currRune), arg)
			}
			continue
		}

		isValue := false
		switch arg.(type) {
		case json.RawMessage:
			isValue = true
		}

		if isValue || driver.IsValue(arg) {
			newArgs = append(newArgs, arg)
			db.appendPlaceholder(&sb, len(newArgs))
			continue
		}

		rv := reflect.ValueOf(arg)
		// log.Printf("Placeholder! %#v %v", arg, rv.IsValid())

		if rv.IsValid() && rv.Type().Kind() == reflect.Slice {
			l := rv.Len()
			if l == 0 {
				return "", nil, fmt.Errorf("replaceArgs: Unable to merge empty slice.")
			}
			sb.WriteRune('(')
			fi := &fieldInfo{ptr: rv.Type().Elem().Kind() == reflect.Ptr}
			for i := 0; i < l; i++ {
				if i > 0 {
					sb.WriteRune(',')
				}
				item := rv.Index(i).Interface()
				if l > db.MaxPlaceholder {
					// append literals
					switch v := item.(type) {
					case string:
						sb.WriteString(db.EscValue(v))
					case int64:
						sb.WriteString(strconv.FormatInt(v, 10))
					default:
						return "", nil, xerrors.Errorf("Unable to add type: %T in slice placeholder. Can only add string and int64", item)
					}
				} else {
					newArgs = append(newArgs, db.nullValue(item, fi))
					db.appendPlaceholder(&sb, len(newArgs))
				}
			}
			sb.WriteRune(')')
			// pretty.Println(parts)
			continue
		}

		newArgs = append(newArgs, arg)
		db.appendPlaceholder(&sb, len(newArgs))

	}

	// append left over args
	for i := nthArg; i < len(args); i++ {
		newArgs = append(newArgs, args[i])
	}

	// log.Printf("%s %v -> \"%s\"", sqlS, args, sb.String())
	return sb.String(), newArgs, nil

}

// appendPlaceholder adds one placeholder to the built
func (db *DB) appendPlaceholder(sb *strings.Builder, numArg int) {
	switch db.PlaceholderMode {
	case QUESTION:
		sb.WriteRune('?')
	case DOLLAR:
		sb.WriteRune('$')
		sb.WriteString(strconv.Itoa(numArg))
	}
}

// nullValue returns the escaped value suitable for UPDATE & INSERT
func (db *DB) nullValue(value interface{}, fi *fieldInfo) interface{} {

	if isZero(value) {
		if fi.allowNull() {
			return nil
		}
		// a pointer which does not allow to store null
		if fi.ptr {
			panic(fmt.Errorf(`Unable to store <nil> pointer in "notnull" field: %s`, fi.name))
		}
	}

	return value
}

// argsToString builds a debug string from given args
func argsToString(args ...interface{}) string {
	var (
		s        string
		sb       strings.Builder
		rv       reflect.Value
		argPrint interface{}
	)
	sb = strings.Builder{}
	for idx, arg := range args {
		if arg == nil {
			sb.WriteString(fmt.Sprintf("#%d <nil>\n", idx))
			continue
		}

		switch arg.(type) {
		case bool, *bool:
			s = "%v"
		case int64, int32, uint64, uint32, int,
			*int64, *int32, *uint64, *uint32, *int:
			s = "%d"
		case float64, float32,
			*float64, *float32:
			s = "%b"
		case string, *string:
			s = "%s"
		default:
			s = "%v"
		}
		rv = reflect.ValueOf(arg)
		argPrint = reflect.Indirect(rv).Interface()
		sb.WriteString(fmt.Sprintf("#%d %s "+s+"\n", idx, rv.Type(), argPrint))
	}
	return sb.String()
}
