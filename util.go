package sqlpro

import (
	"database/sql"
	"database/sql/driver"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"strconv"
	"strings"
	"time"

	"golang.org/x/xerrors"
)

var ErrQueryReturnedZeroRows error = errors.New("query returned 0 rows")

// structInfo is a map to fieldInfo by db_name
type structInfo map[string]*fieldInfo

func (si structInfo) hasDbName(db_name string) bool {
	_, ok := si[db_name]
	return ok
}

func (si structInfo) primaryKey(db_name string) bool {
	fieldInfo, ok := si[db_name]
	if !ok {
		panic(fmt.Sprintf("isPrimaryKey: db_name %s not found", db_name))
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
		return fmt.Errorf("unable to scan time: %T %s", value, value)
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
		return xerrors.Errorf(`sqlpro.NullJson.Scan: Unable to scan type "%T"`, value)
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
	case string:
		if len(v) == 0 {
			return nil
		}
		nj.Data = []byte(v)
		nj.Valid = true
		return nil
	default:
		return xerrors.Errorf("sqlpro.NullRawMessage.Scan: Unable to Scan type %T", value)
	}
}

type fieldInfo struct {
	structField reflect.StructField
	name        string
	dbName      string
	omitEmpty   bool
	primaryKey  bool
	null        bool
	readOnly    bool
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

		path := strings.Split(dbTag, ",")
		if path[0] == "-" {
			// ignore field
			continue
		}

		if field.PkgPath != "" {
			// unexported field
			panic(fmt.Errorf("getStructInfo: Unable to use unexported field for sqlpro: %s", field.Name))
		}

		info := fieldInfo{
			dbName:      path[0],
			structField: field,
			name:        field.Name,
			omitEmpty:   false,
			readOnly:    false,
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
			case "readonly":
				info.readOnly = true
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
			return "", nil, fmt.Errorf("replaceArgs: Expecting #%d arg, Got: %d args", (nthArg + 1), len(args))
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
				return "", nil, fmt.Errorf("replaceArgs: Unable to replace %s with type %T, need *string or string", string(currRune), arg)
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
			db.appendPlaceholder(&sb, len(newArgs)-1)
			continue
		}

		rv := reflect.ValueOf(arg)
		// log.Printf("Placeholder! %#v %v", arg, rv.IsValid())

		if rv.IsValid() && rv.Type().Kind() == reflect.Slice {
			l := rv.Len()
			if l == 0 {
				return "", nil, fmt.Errorf(`sqlpro: replaceArgs: Unable to merge empty slice: "%s"`, sqlS)
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
					db.appendPlaceholder(&sb, len(newArgs)-1)
				}
			}
			sb.WriteRune(')')
			// pretty.Println(parts)
			continue
		}

		newArgs = append(newArgs, arg)
		db.appendPlaceholder(&sb, len(newArgs)-1)

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
		sb.WriteString(strconv.Itoa(numArg + 1))
	}
}

func (db *DB) EscValueForInsert(value interface{}, fi *fieldInfo) string {
	var s string

	v0 := db.nullValue(value, fi)
	if v0 == nil {
		return "NULL"
	}
	switch v := v0.(type) {
	case int:
		return strconv.FormatInt(int64(v), 10)
	case int64:
		return strconv.FormatInt(v, 10)
	case *int64:
		return strconv.FormatInt(*v, 10)
	case float32:
		return strconv.FormatFloat(float64(v), 'f', -1, 32)
	case *float32:
		return strconv.FormatFloat(float64(*v), 'f', -1, 32)
	case float64:
		return strconv.FormatFloat(v, 'f', -1, 64)
	case *float64:
		return strconv.FormatFloat(*v, 'f', -1, 64)
	case bool:
		if v == false {
			return "FALSE"
		} else {
			return "TRUE"
		}
	case *bool:
		if *v == false {
			return "FALSE"
		} else {
			return "TRUE"
		}
	case []uint8:
		s = string(v)
	case json.RawMessage:
		s = string(v)
	case string:
		s = v
	case *string:
		s = *v
	case time.Time:
		s = v.Format(time.RFC3339Nano)
	case *time.Time:
		s = v.Format(time.RFC3339Nano)
	default:
		if vr, ok := v.(driver.Valuer); ok {
			v2, _ := vr.Value()
			return db.EscValueForInsert(v2, fi)
		}
		sv := reflect.ValueOf(value)
		switch sv.Kind() {
		case reflect.Int:
			return strconv.FormatInt(sv.Int(), 10)
		case reflect.String:
			s = sv.String()
		default:
			panic(fmt.Sprintf("EscValueForInsert failed: %T, underlying type: %s", value, sv.Kind()))
		}
	}
	return db.EscValue(s)
}

// nullValue returns the escaped value suitable for UPDATE & INSERT
func (db *DB) nullValue(value interface{}, fi *fieldInfo) interface{} {

	if isZero(value) {
		if fi.allowNull() {
			return nil
		}
		// a pointer which does not allow to store null
		if fi.ptr {
			panic(fmt.Errorf(`unable to store <nil> pointer in "notnull" field: %s`, fi.name))
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
	if len(args) == 0 {
		return " <none>"
	}
	sb = strings.Builder{}
	for idx, arg := range args {
		if arg == nil {
			sb.WriteString(fmt.Sprintf(" #%d <nil>\n", idx))
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
		sb.WriteString(fmt.Sprintf(" #%d %s "+s+"\n", idx, rv.Type(), argPrint))
	}
	return sb.String()
}

func (db *DB) Close() error {
	if db.sqlDB == nil {
		panic("sqlpro.DB.Close: Unable to close, use Open to initialize the wrapper.")
	}
	// log.Printf("sqlpro.Close: %p %s %s", db.DB, db.Driver, db.DSN)
	return db.sqlDB.Close()
}

// Open opens a database connection and returns an sqlpro wrap handle
func Open(driverS, dsn string) (*DB, error) {

	var driver dbDriver

	switch driverS {
	default:
		return nil, fmt.Errorf(`unknown driver "%s"`, driver)
	case "sqlite3":
		driver = SQLITE3
	case "postgres":
		driver = POSTGRES
	}

	conn, err := sql.Open(string(driver), dsn)
	if err != nil {
		return nil, err
	}

	// conn.SetMaxOpenConns(1)

	err = conn.Ping()
	if err != nil {
		conn.Close()
		return nil, err
	}

	wrapper := New(conn)

	wrapper.sqlDB = conn
	wrapper.Driver = driver

	// wrapper.Debug = true

	wrapper.DSN = dsn

	switch driver {
	case POSTGRES:
		wrapper.PlaceholderMode = DOLLAR
		wrapper.UseReturningForLastId = true
		wrapper.SupportsLastInsertId = false
	case SQLITE3:
	default:
		return nil, xerrors.Errorf("sqlpro.Open: Unsupported driver '%s'", driver)
	}

	// log.Printf("sqlpro.Open: %p %s %s", wrapper.DB, driver, dsn)
	return wrapper, nil
}

// Open -> handle
// handle.New -> NewConnection
// handle.Wrap -> Wrap yourself
// handle.Tx -> NewTransaction
// handle.Prepare -> NewPrearedStatement
