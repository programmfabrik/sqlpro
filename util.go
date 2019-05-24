package sqlpro

import (
	"database/sql/driver"
	"fmt"
	"reflect"
	"strings"
	"time"
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

type fieldInfo struct {
	name       string
	dbName     string
	omitEmpty  bool
	primaryKey bool
	null       bool
	notNull    bool
	emptyValue string
	ptr        bool // set true if the field is a pointer
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
			dbName:     path[0],
			name:       field.Name,
			omitEmpty:  false,
			primaryKey: false,
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
		ch, inQuote rune
		nthArg      int
		quoteCount  int
		newArgs     []interface{}
	)

	// pretty.Println(args)

	sb := strings.Builder{}
	// log.Printf("Replace Args: %s %v", sqlS, args)

	for _, ch = range sqlS {

		if ch == db.SingleQuote {
			if inQuote == 0 {
				inQuote = ch
			}
			quoteCount++
		} else {
			if quoteCount > 0 && quoteCount%2 == 0 {
				// quote ends
				inQuote = 0
				quoteCount = 0
			}
		}

		// log.Printf("CH: %s Quote: %s Quote Count: %d", string(ch), string(inQuote), quoteCount)

		if inQuote > 0 {
			sb.WriteRune(ch)
			continue
		}

		quoteCount = 0

		if ch == db.PlaceholderKey {
			arg := args[nthArg]
			nthArg++

			switch v := arg.(type) {
			case *string:
				sb.WriteString(db.Esc(*v))
			case string:
				sb.WriteString(db.Esc(v))
			default:
				return "", nil, fmt.Errorf("replaceArgs: Unable to replace %s with type %T, need *string or string.", string(ch), arg)
			}

			continue
		}

		if ch == db.PlaceholderValue {

			if nthArg >= len(args) {
				return "", nil, fmt.Errorf("replaceArgs: Expecting #%d arg.", nthArg)
			}
			arg := args[nthArg]
			nthArg++

			if driver.IsValue(arg) {
				newArgs = append(newArgs, arg)
			} else {
				rv := reflect.ValueOf(arg)
				parts := make([]string, 0)
				// log.Printf("Placeholder! %#v %v", arg, rv.IsValid())

				if rv.IsValid() && rv.Type().Kind() == reflect.Slice && !driver.IsValue(arg) {
					if rv.Len() == 0 {
						return "", nil, fmt.Errorf("replaceArgs: Unable to merge empty slice.")
					}
					fi := &fieldInfo{ptr: rv.Type().Elem().Kind() == reflect.Ptr}
					for i := 0; i < rv.Len(); i++ {
						item := rv.Index(i).Interface()
						newArgs = append(newArgs, db.escValue(item, fi))
						parts = append(parts, "?")
					}
					sb.WriteString("(" + strings.Join(parts, ",") + ")")
					// pretty.Println(parts)
					continue
				} else {
					newArgs = append(newArgs, arg)
				}
			}
		}
		sb.WriteRune(ch)
	}
	if inQuote > 0 && quoteCount%2 == 1 {
		return "", nil, fmt.Errorf("Unclosed quote %s in \"%s\"", string(inQuote), sqlS)
	}

	// append left over args
	for i := nthArg; i < len(args); i++ {
		newArgs = append(newArgs, args[i])
	}

	// log.Printf("%s %v -> \"%s\"", sqlS, args, sb.String())
	return sb.String(), newArgs, nil

}

// escValue returns the escaped value suitable for UPDATE & INSERT
func (db *DB) escValue(value interface{}, fi *fieldInfo) interface{} {

	if isZero(value) {
		if fi.allowNull() {
			return nil
		}
		// a pointer which does not allow to store null
		if fi.ptr {
			panic("esc Value unimplemented case...")
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
		case bool:
			s = "%v"
		case int64, int32, uint64, uint32, int:
			s = "%d"
		case float64, float32:
			s = "%b"
		default:
			s = "%s"
		}
		rv = reflect.ValueOf(arg)
		argPrint = reflect.Indirect(rv).Interface()
		sb.WriteString(fmt.Sprintf("#%d %s "+s+"\n", idx, rv.Type(), argPrint))
	}
	return sb.String()
}
