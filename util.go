package sqlpro

import (
	"database/sql"
	"database/sql/driver"
	"encoding/json"

	"fmt"
	"reflect"
	"strconv"
	"strings"
	"time"

	_ "github.com/jackc/pgx/v5/stdlib"

	"github.com/pkg/errors"
)

var ErrQueryReturnedZeroRows error = errors.New("Query returned 0 rows")
var ErrMismatchedRowsAffected error = errors.New("Mismatched rows affected")

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
	Time  time.Time
	Valid bool
}

// Scan implements the Scanner interface.
func (ni *NullTime) Scan(value any) error {
	// log.Printf("Scan %T %s", value, value)
	if value == nil {
		ni.Time, ni.Valid = time.Time{}, false
		return nil
	}
	var err error
	switch v := value.(type) {
	case time.Time:
		ni.Time = v
		ni.Valid = true
	case string:
		ni.Time, err = time.Parse(time.RFC3339Nano, v)
		if err != nil {
			return errors.Wrap(err, "NullTime.Scan")
		}
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

func (nj *NullJson) Scan(value any) error {
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
		return errors.Errorf(`sqlpro.NullJson.Scan: Unable to scan type "%T"`, value)
	}
}

type NullRawMessage struct {
	Data  json.RawMessage
	Valid bool
}

func (nj *NullRawMessage) Scan(value any) error {
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
		return errors.Errorf("sqlpro.NullRawMessage.Scan: Unable to Scan type %T", value)
	}
}

type fieldInfo struct {
	structField             reflect.StructField
	name                    string
	dbName                  string
	omitEmpty               bool
	primaryKey              bool
	null                    bool
	readOnly                bool
	notNull                 bool
	isJson, jsonIgnoreError bool
	emptyValue              string
	ptr                     bool // set true if the field is a pointer
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
	si := structInfo{}

	// Resolve anonymous fields
	for i := 0; i < t.NumField(); i++ {
		field := t.Field(i)
		if field.Anonymous {
			if field.Type.Kind() == reflect.Ptr {
				panic(fmt.Sprintf("Unable to scan into embedded pointer type %q", field.Type))
			}

			for dbName, info := range getStructInfo(field.Type) {
				si[dbName] = info
			}
		}
	}

	for i := 0; i < t.NumField(); i++ {
		field := t.Field(i)
		if field.Anonymous {
			// These are resolved above
			continue
		}

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
			case "json_ignore_error":
				info.jsonIgnoreError = true
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

	// logrus.Infof("%s %#v", t.Name(), si)
	return si
}

// replaceArgs rewrites the string sqlS to embed the slice args given
// it returns the new placeholder string and the reduced list of arguments.
func (db2 *db) replaceArgs(sqlS string, args ...any) (string, []any, error) {
	var (
		nthArg, lenRunes int
		newArgs          []any
		sb               strings.Builder
		runes            []rune
		currRune         rune
	)

	// pretty.Println(args)

	sb = strings.Builder{}
	nthArg = 0

	runes = []rune(sqlS)
	lenRunes = len(runes)

	for i := 0; i < lenRunes; i++ {
		currRune = runes[i]
		// skip quoted strings

		if currRune == '\'' || currRune == '"' {
			// forward to the next rune outside the quoted string
			quoteChar := currRune
			sb.WriteRune(currRune)
			i++ // move past opening quote
			for i < lenRunes {
				sb.WriteRune(runes[i])
				if runes[i] == quoteChar {
					// Check for escaped quote (e.g., '' or "")
					if i+1 < lenRunes && runes[i+1] == quoteChar {
						i++
						sb.WriteRune(runes[i]) // write second quote of pair
						i++
						continue
					}
					break // found closing quote
				}
				i++
			}
			continue
		}

		if currRune != db2.PlaceholderKey && currRune != db2.PlaceholderValue {
			sb.WriteRune(currRune)
			continue
		}

		if nthArg >= len(args) {
			return "", nil, fmt.Errorf("replaceArgs: Expecting #%d arg. Got: %d args.", (nthArg + 1), len(args))
		}

		arg := args[nthArg]
		nthArg++

		// replace column and table names ("Key")
		if currRune == db2.PlaceholderKey {
			switch v := arg.(type) {
			case *string:
				sb.WriteString(db2.Esc(*v))
			case string:
				sb.WriteString(db2.Esc(v))
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
			db2.appendPlaceholder(&sb, len(newArgs)-1)
			continue
		}

		rv := reflect.ValueOf(arg)
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
				if l > db2.MaxPlaceholder {
					// append literals
					switch v := item.(type) {
					case string:
						sb.WriteString(db2.EscValue(v))
					case *string:
						if v == nil {
							sb.WriteString("null")
						} else {
							sb.WriteString(db2.EscValue(*v))
						}
					case int:
						sb.WriteString(strconv.FormatInt(int64(v), 10))
					case int32:
						sb.WriteString(strconv.FormatInt(int64(v), 10))
					case int64:
						sb.WriteString(strconv.FormatInt(v, 10))
					case *int:
						if v == nil {
							sb.WriteString("null")
						} else {
							sb.WriteString(strconv.FormatInt(int64(*v), 10))
						}
					case *int32:
						if v == nil {
							sb.WriteString("null")
						} else {
							sb.WriteString(strconv.FormatInt(int64(*v), 10))
						}
					case *int64:
						if v == nil {
							sb.WriteString("null")
						} else {
							sb.WriteString(strconv.FormatInt(*v, 10))
						}
					default:
						return "", nil, errors.Errorf("Unable to add type: %T in slice placeholder. Can only add string, *string, int, int32, int64, *int, *int32  and *int64", item)
					}
				} else {
					newArgs = append(newArgs, db2.nullValue(item, fi))
					db2.appendPlaceholder(&sb, len(newArgs)-1)
				}
			}
			sb.WriteRune(')')
			// pretty.Println(parts)
			continue
		}

		newArgs = append(newArgs, arg)
		db2.appendPlaceholder(&sb, len(newArgs)-1)
	}

	// append left over args
	for i := nthArg; i < len(args); i++ {
		newArgs = append(newArgs, args[i])
	}

	// log.Printf("%s %v -> \"%s\"", sqlS, args, sb.String())
	return sb.String(), newArgs, nil

}

// appendPlaceholder adds one placeholder to the built
func (db2 *db) appendPlaceholder(sb *strings.Builder, numArg int) {
	switch db2.PlaceholderMode {
	case QUESTION:
		sb.WriteRune('?')
	case DOLLAR:
		sb.WriteRune('$')
		sb.WriteString(strconv.Itoa(numArg + 1))
	}
}

func (db2 *db) escValueForInsert(value any, fi *fieldInfo) string {
	vIn := db2.valueForInsert(value, fi)
	switch v := vIn.(type) {
	case string:
		return db2.EscValue(v)
	case float32:
		return strconv.FormatFloat(float64(v), 'f', -1, 32)
	case float64:
		return strconv.FormatFloat(float64(v), 'f', -1, 64)
	case int64:
		return strconv.FormatInt(v, 10)
	case time.Time:
		return db2.EscValue(v.Format(time.RFC3339Nano))
	case bool:
		if v {
			return "TRUE"
		} else {
			return "FALSE"
		}
	case nil:
		return "NULL"
	default:
		panic(fmt.Sprintf("unknown type %T", v))
	}
}
func toTime(v any) (time.Time, bool) {
	rv := reflect.ValueOf(v)
	rt := rv.Type()
	timeType := reflect.TypeOf(time.Time{})

	// Dereference pointer
	if rt.Kind() == reflect.Ptr {
		if rv.IsNil() {
			return time.Time{}, false
		}
		rv = rv.Elem()
		rt = rv.Type()
	}

	// Direct match
	if rt == timeType {
		return rv.Interface().(time.Time), true
	}

	// Convertible types (e.g. SimpleTime)
	if rt.ConvertibleTo(timeType) {
		converted := rv.Convert(timeType) // reflect.Value → time.Time
		return converted.Interface().(time.Time), true
	}

	return time.Time{}, false
}

// valueForInsert returns string, int64, float32, float64, bool or nil
func (db2 *db) valueForInsert(value any, fi *fieldInfo) any {
	var s string

	v0 := db2.nullValue(value, fi)
	if v0 == nil {
		return nil
	}

	switch v := v0.(type) {
	case int:
		return int64(v)
	case *int:
		return int64(*v)
	case int8:
		return int64(v)
	case *int8:
		return int64(*v)
	case int16:
		return int64(v)
	case *int16:
		return int64(*v)
	case int32:
		return int64(v)
	case *int32:
		return int64(*v)
	case int64:
		return v
	case *int64:
		return *v
	case uint:
		return int64(v)
	case *uint:
		return int64(*v)
	case uint8:
		return int64(v)
	case *uint8:
		return int64(*v)
	case uint16:
		return int64(v)
	case *uint16:
		return int64(*v)
	case uint32:
		return int64(v)
	case *uint32:
		return int64(*v)
	case uint64:
		return int64(v)
	case *uint64:
		return int64(*v)
	case float32:
		return v
	case *float32:
		return *v
	case float64:
		return v
	case *float64:
		return *v
	case bool:
		if v == false {
			return false
		} else {
			return true
		}
	case *bool:
		if *v == false {
			return false
		} else {
			return true
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
		return v
	case *time.Time:
		return *v
	default:
		t, isTime := toTime(v0)
		if isTime {
			return t
		}
		vr, ok := value.(driver.Valuer)
		if ok {
			v2, _ := vr.Value()
			return db2.valueForInsert(v2, fi)
		}
		sv := reflect.ValueOf(value)
		// try to use a pointer to check if the driver.Valuer is satisfied
		if sv.Kind() != reflect.Pointer {
			pv := reflect.New(sv.Type())
			pv.Elem().Set(sv)
			var anyVal any = pv.Interface()
			vr2, ok2 := anyVal.(driver.Valuer)
			if ok2 {
				v3, _ := vr2.Value()
				return db2.valueForInsert(v3, fi)
			}
		}
		switch sv.Kind() {
		case reflect.Int:
			return sv.Int()
		case reflect.String:
			s = sv.String()
		default:
			panic(fmt.Sprintf("EscValueForInsert failed: %T value %v in type: %s", value, value, sv.Kind()))
		}
	}
	return s
}

// nullValue returns the escaped value suitable for UPDATE & INSERT
func (db2 *db) nullValue(value any, fi *fieldInfo) any {

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
func argsToString(args ...any) string {
	var (
		s        string
		sb       strings.Builder
		rv       reflect.Value
		argPrint any
	)
	if len(args) == 0 {
		return " <none>"
	}
	sb = strings.Builder{}
	for idx, arg := range args {
		if arg == nil {
			sb.WriteString(fmt.Sprintf(" #%d <nil>\n", idx+1))
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
		sb.WriteString(fmt.Sprintf(" #%d %s "+s+"\n", idx+1, rv.Type(), argPrint))
	}
	return sb.String()
}

func (db2 *db) Close() error {
	if db2.sqlDB == nil {
		panic("sqlpro.DB.Close: Unable to close, use Open to initialize the wrapper")
	}
	if db2.sqlTx != nil {
		panic("sqlpro.TX.Close: Unable to close a tx handle")
	}
	db2.isClosed = true

	// log.Printf("%s sqlpro.Close: %s", db, db.DSN)
	return db2.sqlDB.Close()
}

func (db2 *db) IsClosed() bool {
	if db2 == nil {
		return true
	}
	return db2.isClosed
}

// Open opens a database connection and returns an sqlpro wrap handle
func Open(driverS, dsn string) (DB, error) {

	var driver dbDriver
	var driverName string

	switch driverS {
	default:
		return nil, fmt.Errorf(`Unknown driver "%s"`, driverS)
	case "sqlite3":
		driver = SQLITE3
		driverName = string(driver)
	case "postgres":
		driver = POSTGRES
		driverName = "pgx"
	}

	conn, err := sql.Open(driverName, dsn)
	if err != nil {
		return nil, err
	}

	// conn.SetMaxOpenConns(1)

	err = conn.Ping()
	if err != nil {
		conn.Close()
		return nil, err
	}

	wrapper := newSqlPro(conn)

	wrapper.sqlDB = conn
	wrapper.driver = driver

	// wrapper.Debug = true

	wrapper.DSN = dsn

	switch driver {
	case POSTGRES:
		wrapper.PlaceholderMode = DOLLAR
		wrapper.UseReturningForLastId = true
		wrapper.SupportsLastInsertId = false
	case SQLITE3:
	default:
		return nil, errors.Errorf("sqlpro.Open: Unsupported driver '%s'.", driver)
	}
	return wrapper, nil
}

// Open -> handle
// handle.New -> NewConnection
// handle.Wrap -> Wrap yourself
// handle.Tx -> NewTransaction
// handle.Prepare -> NewPrearedStatement

// IlikeSql returns driver compatible ILIKE where clause snippet. match is
// escaped using %...%. This panics for unknown driver. E.g. "schule" and driver
// "postgres" this will return "ILIKE 'schule'"
func IlikeSql(driver dbDriver, match string) string {
	v := escValue("%"+strings.ReplaceAll(match, `%`, `\%`)+`%`) + ` ESCAPE '\'`
	switch driver {
	default:
		panic(fmt.Errorf("unsupported driver %q", driver))
	case POSTGRES:
		return `ILIKE ` + v
	case SQLITE3:
		return `LIKE ` + v + ` COLLATE NOCASE`
	}
}
