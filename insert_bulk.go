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

// InsertBulk takes a table name and a slice of struct and inserts
// the record in the DB with one Exec.
// The given data needs to be:
//
// *[]*strcut
// *[]struct
// []*struct
// []struct
//
// sqlpro will executes one INSERT statement per call.
func (db *DB) InsertBulk(table string, data interface{}) error {
	rv, structMode, err := checkData(data)
	if err != nil {
		return err
	}

	if structMode {
		return fmt.Errorf("InsertBulk: Need Slice to insert bulk.")
	}

	key_map := make(map[string]*fieldInfo, 0)
	rows := make([]map[string]interface{}, 0)

	for i := 0; i < rv.Len(); i++ {
		row := reflect.Indirect(rv.Index(i)).Interface()

		values, structInfo, err := db.valuesFromStruct(row)

		if err != nil {
			return xerrors.Errorf("sqlpro.InsertBulk error: %w", err)
		}

		rows = append(rows, values)
		for key := range values {
			key_map[key] = structInfo[key]
		}
	}

	insert := strings.Builder{} // make([]string, 0)
	keys := make([]string, 0, len(key_map))

	insert.WriteString("INSERT INTO ")
	insert.WriteString(db.Esc(table))
	insert.WriteString(" (")

	idx := 0
	for key := range key_map {
		if idx > 0 {
			insert.WriteRune(',')
		}
		insert.WriteString(db.Esc(key))
		keys = append(keys, key)
		idx++
	}

	insert.WriteString(") VALUES ")

	for idx, row := range rows {
		if idx > 0 {
			insert.WriteRune(',')
		}
		insert.WriteRune('(')
		for idx2, key := range keys {
			if idx2 > 0 {
				insert.WriteRune(',')
			}
			insert.WriteString(db.EscValueForInsert(row[key], key_map[key]))
		}
		insert.WriteRune(')')
	}

	_, err = db.DB.Exec(insert.String())
	if err != nil {
		return sqlError(err, insert.String(), []interface{}{})
	}

	return nil
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
