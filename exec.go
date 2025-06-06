package sqlpro

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"log"
	"reflect"
	"strings"

	"github.com/lib/pq"
	"github.com/pkg/errors"
	"github.com/programmfabrik/golib"
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

func checkData(data interface{}) (rv reflect.Value, structMode bool, err error) {
	erro := func() (reflect.Value, bool, error) {
		return rv, false, fmt.Errorf("Insert/Update needs a struct or slice of structs.")
	}

	rv = reflect.Indirect(reflect.ValueOf(data))
	switch rv.Type().Kind() {
	case reflect.Slice:
		switch rv.Type().Elem().Kind() {
		case reflect.Ptr:
			if rv.Type().Elem().Elem().Kind() != reflect.Struct {
				return erro()
			}
		case reflect.Interface, reflect.Struct:
		default:
			return rv, false, fmt.Errorf("Insert/Update needs a slice of structs. Have: %s", rv.Type().Elem().Kind())
		}
	case reflect.Struct:
		structMode = true
	default:
		return erro()
	}

	return rv, structMode, nil
}

func (db *DB) Insert(table string, data interface{}) error {
	return db.InsertContext(context.Background(), table, data)
}

// Insert takes a table name and a struct and inserts
// the record in the DB.
// The given data needs to be:
//
// *[]*strcut
// *[]struct
// []*struct
// []struct
// struct
// *struct
//
// sqlpro will executes one INSERT statement per row.
// result.LastInsertId will be used to set the first primary
// key column.

func (db *DB) InsertContext(ctx context.Context, table string, data interface{}) error {
	var (
		rv         reflect.Value
		structMode bool
		err        error
	)

	rv, structMode, err = checkData(data)
	if err != nil {
		return err
	}

	if !structMode {
		for i := 0; i < rv.Len(); i++ {
			row := reflect.Indirect(rv.Index(i))
			insert_id, structInfo, err := db.insertStruct(ctx, table, row.Interface())
			if err != nil {
				return err
			}
			pk := structInfo.onlyPrimaryKey()
			if pk != nil && pk.structField.Type.Kind() == reflect.Int64 {
				setPrimaryKey(row.FieldByName(pk.name), insert_id)
			}
		}
	} else {
		insert_id, structInfo, err := db.insertStruct(ctx, table, rv.Interface())
		if err != nil {
			return err
		}
		pk := structInfo.onlyPrimaryKey()
		// log.Printf("PK: %d", insert_id)
		if pk != nil && rv.CanAddr() {
			switch pk.structField.Type.Kind() {
			case reflect.Int,
				reflect.Int8,
				reflect.Int16,
				reflect.Int32,
				reflect.Int64,
				reflect.Uint,
				reflect.Uint8,
				reflect.Uint16,
				reflect.Uint32,
				reflect.Uint64:
				setPrimaryKey(rv.FieldByName(pk.name), insert_id)
			}
		}
	}

	// data
	return nil
}

func setPrimaryKey(rv reflect.Value, id int64) {
	switch rv.Type().Kind() {
	case reflect.Int,
		reflect.Int8,
		reflect.Int16,
		reflect.Int32,
		reflect.Int64:
		rv.SetInt(id)
	case reflect.Uint,
		reflect.Uint8,
		reflect.Uint16,
		reflect.Uint32,
		reflect.Uint64:
		rv.SetUint(uint64(id))
	default:
		err := fmt.Errorf("Unknown type to set primary key: %s", rv.Type())
		panic(err)
	}
}

func (db *DB) InsertBulk(table string, data interface{}) error {
	return db.InsertBulkContext(context.Background(), table, data)
}

// InsertBulk takes a table name and a slice of struct and inserts
// the record in the DB with one Exec.
// The given data needs to be:
//
// *[]*struct
// *[]struct
// []*struct
// []struct
//
// sqlpro will executes one INSERT statement per call.
func (db *DB) InsertBulkContext(ctx context.Context, table string, data interface{}) error {
	return db.insertBulkContext(ctx, table, data, false, nil)
}

// InsertBulkOnConflictDoNothingContext works like InsertBulkContext but adds a
// "ON CONFLICT DO NOTHING" to the insert command.
func (db *DB) InsertBulkOnConflictDoNothingContext(ctx context.Context, table string, data interface{}, cols ...string) error {
	return db.insertBulkContext(ctx, table, data, true, cols)
}

func (db *DB) insertBulkContext(ctx context.Context, table string, data interface{}, onConflictDoNothing bool, conflictCols []string) error {
	var (
		rv         reflect.Value
		structMode bool
		err        error
	)

	rv, structMode, err = checkData(data)
	if err != nil {
		return err
	}

	if structMode {
		return fmt.Errorf("InsertBulk: Need Slice to insert bulk.")
	}

	key_map := make(map[string]*fieldInfo, 0)
	rows := make([]map[string]interface{}, 0)

	if rv.Len() == 0 {
		return nil
	}

	for i := 0; i < rv.Len(); i++ {
		row := reflect.Indirect(rv.Index(i)).Interface()

		values, structInfo, err := db.valuesFromStruct(row)

		if err != nil {
			return errors.Wrap(err, "sqlpro.InsertBulk error.")
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

	insert.WriteString(") VALUES \n")

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
		insert.WriteRune('\n')
	}

	if onConflictDoNothing {
		if len(conflictCols) > 0 {
			cCols := []string{}
			for _, cc := range conflictCols {
				cCols = append(cCols, db.Esc(cc))
			}
			insert.WriteString(" ON CONFLICT (" + strings.Join(cCols, ",") + ") DO NOTHING")
		} else {
			insert.WriteString(" ON CONFLICT DO NOTHING")
		}
	}

	rowsAffected, _, err := db.execContext(ctx, insert.String())
	if !onConflictDoNothing && err == nil && rowsAffected != int64(len(rows)) {
		err = ErrMismatchedRowsAffected
	}
	if err != nil {
		return db.sqlError(err, insert.String(), []interface{}{})
	}

	return nil
}

func (db *DB) UpdateBulk(table string, data interface{}) error {
	return db.UpdateBulkContext(context.Background(), table, data)
}

// UpdateBulkContext updates all records of the passed slice. It using a single
// exec to send the data to the database. This is generally faster than calling Update
// with a slice (which sends individual update requests).
func (db *DB) UpdateBulkContext(ctx context.Context, table string, data interface{}) error {
	var (
		rv         reflect.Value
		structMode bool
		err        error
	)

	rv, structMode, err = checkData(data)
	if err != nil {
		return err
	}

	if structMode {
		return fmt.Errorf("UpdateBulk: Need Slice to update bulk.")
	}

	l := rv.Len()
	if l == 0 {
		return nil
	}

	update := strings.Builder{} // make([]string, 0)
	for i := 0; i < l; i++ {
		row := reflect.Indirect(rv.Index(i)).Interface()
		values, structInfo, err := db.valuesFromStruct(row)
		if err != nil {
			return errors.Wrap(err, "sqlpro.UpdateBulk error.")
		}
		where := strings.Builder{}
		whereCount := 0
		update.WriteString("UPDATE ")
		update.WriteString(db.Esc(table))
		update.WriteString(" SET ")
		idx2 := 0
		for key, value := range values {
			value2 := db.nullValue(value, structInfo[key])
			if structInfo[key].primaryKey {
				// skip primary keys for update
				if value2 == nil {
					return fmt.Errorf("Unable to build UPDATE clause with <nil> primary key: %s", key)
				}
				if whereCount > 0 {
					where.WriteString(" AND ")
				}
				where.WriteString(db.Esc(key))
				where.WriteRune('=')
				where.WriteString(db.EscValueForInsert(value2, structInfo[key]))
				whereCount++
			} else {
				if idx2 > 0 {
					update.WriteRune(',')
				}
				idx2++
				update.WriteString(db.Esc(key))
				update.WriteRune('=')
				update.WriteString(db.EscValueForInsert(value2, structInfo[key]))
			}
		}
		update.WriteString(" WHERE ")
		update.Write([]byte(where.String()))
		update.WriteRune(';')
		update.WriteRune('\n')
	}

	rowsAffected, _, err := db.execContext(ctx, update.String())
	if err == nil && rowsAffected != 1 {
		err = ErrMismatchedRowsAffected
	}
	if err != nil {
		return db.sqlError(err, update.String(), []interface{}{})
	}

	return nil
}

func (db *DB) InsertBulkCopyIn(table string, data interface{}) error {
	var (
		rv         reflect.Value
		structMode bool
		err        error
	)

	rv, structMode, err = checkData(data)
	if err != nil {
		return err
	}

	if structMode {
		return fmt.Errorf("InsertBulk: Need Slice to insert bulk.")
	}

	key_map := make(map[string]*fieldInfo, 0)
	rows := make([]map[string]interface{}, 0)

	if rv.Len() == 0 {
		return nil
	}

	for i := 0; i < rv.Len(); i++ {
		row := reflect.Indirect(rv.Index(i)).Interface()

		values, structInfo, err := db.valuesFromStruct(row)

		if err != nil {
			return errors.Wrap(err, "sqlpro.InsertBulk error.")
		}

		rows = append(rows, values)
		for key := range values {
			key_map[key] = structInfo[key]
		}
	}

	txn, err := db.sqlDB.Begin()
	if err != nil {
		return db.sqlError(err, "BEGIN TRANSACTION", []interface{}{})
	}

	keys := make([]string, 0, len(key_map))
	for key := range key_map {
		keys = append(keys, key)
	}

	stmt, err := txn.Prepare(pq.CopyIn(table, keys...))
	if err != nil {
		return db.sqlError(err, "Prepare", []interface{}{})
	}

	for _, row := range rows {
		values := make([]interface{}, 0, len(key_map))
		for _, key := range keys {
			values = append(values, row[key])
		}
		_, err = stmt.Exec(values...)
		if err != nil {
			return db.sqlError(err, "Exec", values)
		}
	}

	_, err = stmt.Exec()
	if err != nil {
		return db.sqlError(err, "Exec DONE", []interface{}{})
	}

	err = txn.Commit()
	if err != nil {
		return db.sqlError(err, "Commit DONE", []interface{}{})
	}

	return nil
}

func (db *DB) insertStruct(ctx context.Context, table string, row interface{}) (int64, structInfo, error) {
	values, info, err := db.valuesFromStruct(row)
	if err != nil {
		return 0, nil, err
	}

	sql, args, err := db.insertClauseFromValues(table, values, info)
	if err != nil {
		return 0, nil, err
	}

	if db.UseReturningForLastId {
		pk := info.onlyPrimaryKey()
		if pk != nil {
			// Fail if transaction present and not in write mode
			if db.sqlTx != nil && !db.txWriteMode {
				return 0, nil, fmt.Errorf("[%s] Trying to write into read-only transaction: %s", db, sql)
			}

			sql = sql + " RETURNING " + db.Esc(pk.dbName)
			var insert_id_any interface{}
			if db.Debug || db.DebugExec {
				log.Printf("%s SQL: %s\nARGS:\n%s", db, golib.CutStr(sql, 2000, "..."), argsToString(args...))
			}
			err := db.QueryContext(ctx, &insert_id_any, sql, args...)
			if err != nil {
				return 0, nil, err
			}
			insert_id, _ := insert_id_any.(int64) // ignore conversion error, return 0 in that case
			// log.Printf("Returning ID: %T %v", insert_id_any, insert_id_any)
			return insert_id, info, nil
		}
	}

	// log.Printf("SQL: %s Debug: %v", sql, db.Debug)
	rowsAffected, insert_id, err := db.execContext(ctx, sql, args...)
	if err == nil && rowsAffected != 1 {
		err = ErrMismatchedRowsAffected
	}
	if err != nil {
		return 0, nil, err
	}

	return insert_id, info, nil
}

func (db *DB) insertClauseFromValues(table string, values map[string]interface{}, info structInfo) (string, []interface{}, error) {
	cols := make([]string, 0, len(values))
	vs := make([]string, 0, len(values))
	args := make([]interface{}, 0, len(values))

	for col, value := range values {
		cols = append(cols, db.Esc(col))
		vs = append(vs, "?")
		args = append(args, db.nullValue(value, info[col]))
	}
	return fmt.Sprintf("INSERT INTO %s (%s) VALUES(%s)",
		db.Esc(table),
		strings.Join(cols, ","),
		strings.Join(vs, ","),
	), args, nil
}

func (db *DB) updateClauseFromRow(table string, row interface{}) (string, []interface{}, error) {

	var (
		valid     bool
		args      []interface{}
		whereArgs []interface{}
		pk_value  interface{}
	)

	values, structInfo, err := db.valuesFromStruct(row)
	if err != nil {
		return "", nil, err
	}

	update := strings.Builder{}
	where := strings.Builder{}

	update.WriteString("UPDATE ")
	update.WriteString(db.Esc(table))
	update.WriteString(" SET ")

	where.WriteString(" WHERE ")

	for key, value := range values {
		if structInfo.primaryKey(key) {
			// skip primary keys for update
			pk_value = db.nullValue(value, structInfo[key])
			if pk_value == nil {
				return "", args, fmt.Errorf("Unable to build UPDATE clause with <nil> key: %s", key)
			}
			if len(whereArgs) > 0 {
				where.WriteString(" AND ")
			}
			where.WriteString(db.Esc(key))
			where.WriteString("=")
			where.WriteRune(db.PlaceholderValue)

			whereArgs = append(whereArgs, pk_value)
			valid = true
		} else {
			if len(args) > 0 {
				update.WriteString(",")
			}
			update.WriteString(db.Esc(key))
			update.WriteString("=")
			update.WriteRune(db.PlaceholderValue)
			args = append(args, db.nullValue(value, structInfo[key]))
		}
	}

	if !valid {
		return "", args, fmt.Errorf("Unable to build UPDATE clause, at least one key needed.")
	}

	args = append(args, whereArgs...)

	// Add where clause
	return update.String() + where.String(), args, nil
}

func (db *DB) Update(table string, data interface{}) error {
	return db.UpdateContext(context.Background(), table, data)
}

// Update updates the given struct or slice of structs
// The WHERE clause is put together from the "pk" columns.
// If not all "pk" columns have non empty values, Update returns
// an error.
func (db *DB) UpdateContext(ctx context.Context, table string, data interface{}) error {
	var (
		rv         reflect.Value
		structMode bool
		err        error
		update     string
		args       []interface{}
	)

	if db == nil {
		panic("Update on <nil> handle.")
	}

	rv, structMode, err = checkData(data)
	if err != nil {
		return err
	}

	if structMode {
		update, args, err = db.updateClauseFromRow(table, rv.Interface())
		if err != nil {
			return err
		}
		rowsAffected, _, err := db.execContext(ctx, update, args...)
		if err == nil && rowsAffected != 1 {
			err = ErrMismatchedRowsAffected
		}
		if err != nil {
			return err
		}
	} else {
		for i := 0; i < rv.Len(); i++ {
			row := reflect.Indirect(rv.Index(i))
			update, args, err = db.updateClauseFromRow(table, row.Interface())
			if err != nil {
				return err
			}
			rowsAffected, _, err := db.execContext(ctx, update, args...)
			if err == nil && rowsAffected != 1 {
				err = ErrMismatchedRowsAffected
			}
			if err != nil {
				return err
			}
		}
	}

	return nil
}

// Save saves the given data. It performs an INSERT if the only primary key is
// zero, and and UPDATE if it is not. It panics if it the record has no primary
// key or less than one
func (db *DB) Save(table string, data interface{}) error {

	rv, structMode, err := checkData(data)
	if err != nil {
		return err
	}

	if structMode {
		return db.saveRow(table, data)
	} else {
		for i := 0; i < rv.Len(); i++ {
			err = db.saveRow(table, rv.Index(i).Interface())
			if err != nil {
				return err
			}
		}
	}

	return nil
}

func (db *DB) saveRow(table string, data interface{}) error {
	row := reflect.Indirect(reflect.ValueOf(data))

	values, info, err := db.valuesFromStruct(row.Interface())
	if err != nil {
		return err
	}
	pk := info.onlyPrimaryKey()

	if pk == nil {
		return fmt.Errorf("Save needs a struct with exactly one 'pk' field.")
	}

	pk_value, ok := values[pk.dbName]

	if !ok || isZero(pk_value) {
		return db.Insert(table, data)
	} else {
		return db.Update(table, data)
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
				actualData = reflect.Zero(fieldInfo.structField.Type).Interface()
			}
			actualData, err = json.Marshal(actualData)
			if err != nil {
				return nil, nil, errors.Wrap(err, "Unable to marshal as data as json.")
			}
			// If the database accepts "null" we write NULL, if the db does not accept null
			// we write "null", if it is not specified we write NULL if the json renders to "null"
			if isZero && (fieldInfo.null || !fieldInfo.notNull && string(actualData.([]byte)) == "null") {
				actualData = nil
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

// execContext wraps DB.Exec and returns the number of affected rows as reported
// by the driver as well as the ID inserted, if the driver supports it.
func (db *DB) execContext(ctx context.Context, execSql string, args ...interface{}) (rowsAffected, insertID int64, err error) {
	var (
		execSql0 string
		newArgs  []interface{}
	)

	if db.txExecQueryMtx != nil {
		db.txExecQueryMtx.Lock()
		defer db.txExecQueryMtx.Unlock()
	}

	if db.Debug || db.DebugExec {
		log.Printf("%s SQL: %s\nARGS:\n%s", db, golib.CutStr(execSql, 2000, "..."), argsToString(args...))
	}

	// Fail if transaction present and not in write mode
	if db.sqlTx != nil && !db.txWriteMode {
		return 0, 0, fmt.Errorf("[%s] Trying to write into read-only transaction: %s", db, execSql)
	}

	if len(args) > 0 {
		execSql0, newArgs, err = db.replaceArgs(execSql, args...)
		if err != nil {
			return 0, 0, err
		}
	} else {
		execSql0 = execSql
		newArgs = args
	}

	// logrus.Infof("[%p] EXEC #%d %s %s", db.sqlDB, db.transID, aurora.Green(fmt.Sprintf("%p", db.db)), execSql0[0:10])

	var result sql.Result

	// tries := 0
	for {
		result, err = db.db.ExecContext(ctx, execSql0, newArgs...)
		if err != nil {
			// pp.Println(err)
			// sqlErr, ok := err.(sqlite3.Error)
			// if ok {
			// 	if sqlErr.Code == 5 { // SQLITE_BUSY
			// 		tries++
			// 		time.Sleep(50 * time.Millisecond)
			// 		if tries < 3 {
			// 			continue
			// 		}
			// 	}
			// }
			return 0, 0, db.debugError(db.sqlError(err, execSql0, newArgs))
		}
		break
	}

	row_count, err := result.RowsAffected()
	if err != nil {
		// Ignore the error here, we might get
		// no RowsAffected available after the empty statement from pq driver
		// which is ok and not a real error (it happens with empty statements)
	}

	if !db.SupportsLastInsertId {
		return row_count, 0, nil
	}

	last_insert_id, err := result.LastInsertId()
	if err != nil {
		return row_count, 0, db.debugError(err)
	}
	return row_count, last_insert_id, nil
}
