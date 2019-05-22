package sqlpro

import (
	"fmt"
	"log"
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
	if value == nil {
		ni.Time, ni.Valid = nil, false
		return nil
	}
	log.Printf("Scan Time: %T", value)
	ni.Valid = true
	return nil

}

type fieldInfo struct {
	name       string
	dbName     string
	omitEmpty  bool
	primaryKey bool
	null       bool
	ptr        bool // set true if the field is a pointer
}

// getStructInfo returns a per dbName to fieldInfo map
func getStructInfo(t reflect.Type) structInfo {
	si := make(structInfo, 0)

	// log.Printf("name: %s %d", t, t.NumField())
	for i := 0; i < t.NumField(); i++ {
		field := t.Field(i)
		if field.PkgPath != "" {
			// unexported field
			continue
		}
		dbTag := field.Tag.Get("db")
		if dbTag == "" {
			// ignore field
			continue
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

		info.ptr = field.Type.Kind() == reflect.Ptr

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
			default:
				// ignore unrecognized
			}
		}
		si[info.dbName] = &info
	}
	return si
}

// replaceArgs rewrites the string sqlS to embed the args given
func (db *DB) replaceArgs(sqlS string, args ...interface{}) (string, error) {
	var (
		err         error
		ch, inQuote rune
		nthArg      int
		s           string
		quoteCount  int
	)

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

		if ch == '?' {
			if nthArg >= len(args) {
				return "", fmt.Errorf("replaceArgs: Expecting #%d arg.", nthArg)
			}
			arg := args[nthArg]
			s, err = db.escapeArg(arg)
			if err != nil {
				return "", err
			}
			sb.WriteString(s)
			nthArg++
			continue
		}
		sb.WriteRune(ch)
	}
	if inQuote > 0 && quoteCount%2 == 1 {
		return "", fmt.Errorf("Unclosed quote %s in \"%s\"", string(inQuote), sqlS)
	}

	// log.Printf("%s %v -> \"%s\"", sqlS, args, sb.String())
	return sb.String(), nil

}

// arg returns the escape argument
func (db *DB) escapeArg(arg interface{}) (string, error) {
	switch v := arg.(type) {
	case []int64:
		var parts []string
		for _, v0 := range v {
			escV, err := db.escValue(v0, &fieldInfo{ptr: false})
			if err != nil {
				return "", err
			}
			parts = append(parts, escV)
		}
		return "(" + strings.Join(parts, ",") + ")", nil
	case []string:
		var parts []string
		for _, v0 := range v {
			escV, err := db.escValue(v0, &fieldInfo{ptr: false})
			if err != nil {
				return "", err
			}
			parts = append(parts, escV)
		}
		return "(" + strings.Join(parts, ",") + ")", nil
	default:
		return db.escValue(arg, &fieldInfo{ptr: false})
	}
}

// escValue returns the escaped value suitable for UPDATE & INSERT
func (db *DB) escValue(value interface{}, fi *fieldInfo) (string, error) {

	if isZero(value) {
		if fi.ptr {
			return "null", nil // write NULL if we use a pointer
		}
		if fi.null {
			return "null", nil // write NULL only if it is explicitly set
		}
	}

	switch v := value.(type) {
	case string:
		return "'" + strings.ReplaceAll(v, "'", "''") + "'", nil
	case *string:
		return "'" + strings.ReplaceAll(*v, "'", "''") + "'", nil
	case int64, *int64, int32, *int32, uint64, *uint64, uint32, *uint32, int, *int:
		return fmt.Sprintf("%d", value), nil
	case float64, *float64, float32, *float32, *bool, bool:
		return fmt.Sprintf("%v", value), nil
	case time.Time:
		return fmt.Sprintf("'%s'", v.Format(time.RFC3339Nano)), nil
	case *time.Time:
		return fmt.Sprintf("'%s'", (*v).Format(time.RFC3339Nano)), nil
	default:
		rv := reflect.ValueOf(value)
		if rv.Type().Kind() == reflect.Struct {
			log.Printf("Unable to store struct: %s", rv.Type())
			return "null", nil
		}
		// as fallback we use Sprintf to case everything else
		// log.Printf("Casting: %T: %v", value, value)
		s := fmt.Sprintf("%s", value)
		return "'" + strings.ReplaceAll(s, "'", "''") + "'", nil
	}
}
