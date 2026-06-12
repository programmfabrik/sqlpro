package sqlpro

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"log"
	"maps"
	"reflect"
	"slices"
	"strings"
	"sync"
	"sync/atomic"

	"github.com/jackc/pgx/v5"
	"github.com/pkg/errors"
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

func checkData(data any) (rv reflect.Value, structMode bool, err error) {
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

func (db2 *db) Insert(table string, data any) error {
	return db2.InsertContext(context.Background(), table, data)
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

func (db2 *db) InsertContext(ctx context.Context, table string, data any) error {
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
			insert_id, structInfo, err := db2.insertStruct(ctx, table, row.Interface())
			if err != nil {
				return err
			}
			pk := structInfo.onlyPrimaryKey()
			if pk != nil && pk.structField.Type.Kind() == reflect.Int64 {
				setPrimaryKey(row.FieldByIndex(pk.structField.Index), insert_id)
			}
		}
	} else {
		insert_id, structInfo, err := db2.insertStruct(ctx, table, rv.Interface())
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
				setPrimaryKey(rv.FieldByIndex(pk.structField.Index), insert_id)
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

func (db2 *db) InsertBulk(table string, data any) error {
	return db2.InsertBulkContext(context.Background(), table, data)
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
func (db2 *db) InsertBulkContext(ctx context.Context, table string, data any) error {
	return db2.insertBulkContext(ctx, table, data, false, nil)
}

// InsertBulkOnConflictDoNothingContext works like InsertBulkContext but adds a
// "ON CONFLICT DO NOTHING" to the insert command.
func (db2 *db) InsertBulkOnConflictDoNothingContext(ctx context.Context, table string, data any, cols ...string) error {
	return db2.insertBulkContext(ctx, table, data, true, cols)
}

type copyFromData struct {
	keyMap      map[string]*fieldInfo
	columns     []string
	rv          reflect.Value // the slice of struct rows
	plan        []*fieldInfo  // per column fieldInfo of the current row type
	lastType    reflect.Type
	values      []any // reused across rows
	nextCounter int
	db          *db
}

func newCopyFromData(db *db, keyMap map[string]*fieldInfo, columns []string, rv reflect.Value) (cfd *copyFromData) {
	return &copyFromData{
		keyMap:      keyMap,
		columns:     columns,
		rv:          rv,
		plan:        make([]*fieldInfo, len(columns)),
		values:      make([]any, len(columns)),
		nextCounter: -1,
		db:          db,
	}
}

func (cfd copyFromData) Columns() []string {
	return cfd.columns
}

func (cfd copyFromData) Len() int64 {
	return int64(cfd.rv.Len())
}

func (cfd *copyFromData) Next() bool {
	if cfd.rv.Len()-1 > cfd.nextCounter {
		cfd.nextCounter++
		return true
	}
	return false
}

func (cfd copyFromData) Err() error {
	return nil
}

func (cfd *copyFromData) Values() (values []any, err error) {
	defer func() {
		r := recover()
		if r == nil {
			return
		}
		err = fmt.Errorf("panic: %v", r)
	}()

	dataV := bulkRowBoxed(cfd.rv, cfd.nextCounter)
	if t := dataV.Type(); t != cfd.lastType {
		cfd.lastType = t
		bindBulkPlan(t, cfd.columns, cfd.plan)
	}
	for idx, fi := range cfd.plan {
		var value any
		if fi != nil {
			value, err = cfd.db.fieldValueForBulk(dataV, fi)
			if err != nil {
				return nil, err
			}
		} else {
			// column not present on this row's type: render NULL with the
			// fieldInfo of the type that introduced the column
			fi = cfd.keyMap[cfd.columns[idx]]
		}
		cfd.values[idx] = cfd.db.valueForInsert(value, fi)
	}
	return cfd.values, nil
}

func (db2 *db) copyFrom(ctx context.Context, table string, data *copyFromData) error {
	pgxConn := db2.pgxConn()
	if pgxConn == nil {
		panic("copyFrom needs pgx connection")
	}

	rowsAffected, err := pgxConn.CopyFrom(ctx, pgx.Identifier{table}, data.Columns(), data)
	if err != nil {
		return err
	}
	if rowsAffected != data.Len() {
		return ErrMismatchedRowsAffected
	}
	return nil
}

// bulkRow returns the i-th row of the slice rv as a struct value,
// dereferencing pointers and unwrapping interface elements.
func bulkRow(rv reflect.Value, i int) reflect.Value {
	dataV := reflect.Indirect(rv.Index(i))
	if dataV.Kind() == reflect.Interface {
		dataV = dataV.Elem()
	}
	return dataV
}

// bulkRowBoxed returns the i-th row like bulkRow, but detached from the slice
// (one boxed copy). Reading fields of the detached row with Interface() shares
// that copy instead of allocating a defensive copy per field.
func bulkRowBoxed(rv reflect.Value, i int) reflect.Value {
	return reflect.ValueOf(bulkRow(rv, i).Interface())
}

// bindBulkPlan fills plan with, for each column key, the writable fieldInfo of
// the row type t, or nil if t has no writable field for that column (such
// cells render as NULL).
func bindBulkPlan(t reflect.Type, keys []string, plan []*fieldInfo) {
	info := getStructInfo(t)
	for i, key := range keys {
		fi := info[key]
		if fi != nil && fi.readOnly {
			fi = nil
		}
		plan[i] = fi
	}
}

// fieldValueForBulk extracts the value of field fi from the struct dataV,
// applying the same omitempty/json rules as valuesFromStruct. An omitted
// (omitempty + zero) field yields nil, which renders as NULL.
func (db2 *db) fieldValueForBulk(dataV reflect.Value, fi *fieldInfo) (any, error) {
	fieldV := dataV.FieldByIndex(fi.structField.Index)
	actualData := fieldV.Interface()
	isZero := isZeroValue(fieldV)

	if isZero && fi.omitEmpty {
		return nil, nil
	}

	if fi.isJson {
		if isZero {
			actualData = reflect.Zero(fi.structField.Type).Interface()
		}
		var err error
		actualData, err = json.Marshal(actualData)
		if err != nil {
			if !fi.jsonIgnoreError {
				return nil, errors.Wrap(err, "Unable to marshal as data as json.")
			}
		}
		// If the database accepts "null" we write NULL, if the db does not accept null
		// we write "null", if it is not specified we write NULL if the json renders to "null"
		if isZero && (fi.null || !fi.notNull && string(actualData.([]byte)) == "null") {
			actualData = nil
		}
	}
	return actualData, nil
}

func (db2 *db) insertBulkContext(ctx context.Context, table string, data any, onConflictDoNothing bool, conflictCols []string) (err error) {
	var (
		rv         reflect.Value
		structMode bool
	)

	rv, structMode, err = checkData(data)
	if err != nil {
		return err
	}

	if structMode {
		return fmt.Errorf("InsertBulk: Need Slice to insert bulk.")
	}

	l := rv.Len()
	if l == 0 {
		return nil
	}

	// Pass 1: determine the column set without materializing any values.
	// A column is included if it is not read-only and either is not tagged
	// omitempty or has a non-zero value in at least one row.
	useCopyFrom := !onConflictDoNothing && db2.pgxConn() != nil
	var (
		key_map    = map[string]*fieldInfo{}
		keys       = []string{}
		lastType   reflect.Type
		pending    []*fieldInfo // omitempty fields of lastType not yet included
		jsonFields []*fieldInfo // json fields of lastType to pre-validate for COPY
	)
	addKey := func(fi *fieldInfo) {
		if _, ok := key_map[fi.dbName]; !ok {
			keys = append(keys, fi.dbName)
		}
		key_map[fi.dbName] = fi
	}
	for i := 0; i < l; i++ {
		dataV := bulkRow(rv, i)
		if t := dataV.Type(); t != lastType {
			lastType = t
			pending = pending[:0]
			jsonFields = jsonFields[:0]
			for _, fi := range getStructInfo(t) {
				if fi.readOnly {
					continue
				}
				if !fi.omitEmpty {
					addKey(fi)
				} else if _, ok := key_map[fi.dbName]; !ok {
					pending = append(pending, fi)
				}
				if useCopyFrom && fi.isJson && !fi.jsonIgnoreError {
					jsonFields = append(jsonFields, fi)
				}
			}
		}
		for j := 0; j < len(pending); {
			fi := pending[j]
			if !isZeroValue(dataV.FieldByIndex(fi.structField.Index)) {
				addKey(fi)
				pending = append(pending[:j], pending[j+1:]...)
				continue
			}
			j++
		}
		// On the COPY path json marshaling happens while the COPY is already
		// streaming; an error there would abort it mid-stream and poison the
		// surrounding transaction. Validate json fields up front so a bad row
		// fails before any SQL is sent, like the literal path does.
		for _, fi := range jsonFields {
			if _, err := db2.fieldValueForBulk(dataV, fi); err != nil {
				return fmt.Errorf("sqlpro.insertBulkContext: %w", err)
			}
		}
	}

	// Use faster COPY FROM for postgres if possible
	if useCopyFrom {
		return db2.copyFrom(ctx, table, newCopyFromData(db2, key_map, keys, rv))
	}

	insert := strings.Builder{} // make([]string, 0)

	insert.WriteString("INSERT INTO ")
	insert.WriteString(db2.Esc(table))
	insert.WriteString(" (")

	for idx, key := range keys {
		if idx > 0 {
			insert.WriteRune(',')
		}
		insert.WriteString(db2.Esc(key))
	}

	insert.WriteString(") VALUES \n")

	// Pass 2: stream the values of every row directly into the SQL.
	plan := make([]*fieldInfo, len(keys))
	lastType = nil
	for i := 0; i < l; i++ {
		dataV := bulkRowBoxed(rv, i)
		if t := dataV.Type(); t != lastType {
			lastType = t
			bindBulkPlan(t, keys, plan)
		}
		if i > 0 {
			insert.WriteRune(',')
		}
		insert.WriteRune('(')
		for j, fi := range plan {
			if j > 0 {
				insert.WriteRune(',')
			}
			var value any
			if fi != nil {
				value, err = db2.fieldValueForBulk(dataV, fi)
				if err != nil {
					return fmt.Errorf("sqlpro.insertBulkContext: %w", err)
				}
			} else {
				// column not present on this row's type: render NULL with the
				// fieldInfo of the type that introduced the column
				fi = key_map[keys[j]]
			}
			insert.WriteString(db2.escValueForInsert(value, fi))
		}
		insert.WriteRune(')')
		insert.WriteRune('\n')
	}

	if onConflictDoNothing {
		if len(conflictCols) > 0 {
			cCols := []string{}
			for _, cc := range conflictCols {
				cCols = append(cCols, db2.Esc(cc))
			}
			insert.WriteString(" ON CONFLICT (" + strings.Join(cCols, ",") + ") DO NOTHING")
		} else {
			insert.WriteString(" ON CONFLICT DO NOTHING")
		}
	}

	rowsAffected, _, err := db2.execContext(ctx, insert.String())
	if !onConflictDoNothing && err == nil && rowsAffected != int64(l) {
		err = ErrMismatchedRowsAffected
	}
	if err != nil {
		return db2.sqlError(err, insert.String(), []any{})
	}

	return nil
}

func (db2 *db) UpdateBulk(table string, data any) error {
	return db2.UpdateBulkContext(context.Background(), table, data)
}

// UpdateBulkContext updates all records of the passed slice. It using a single
// exec to send the data to the database. This is generally faster than calling Update
// with a slice (which sends individual update requests).
func (db2 *db) UpdateBulkContext(ctx context.Context, table string, data any) error {
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
		values, structInfo, err := db2.valuesFromStruct(row)
		if err != nil {
			return errors.Wrap(err, "sqlpro.UpdateBulk error.")
		}
		where := strings.Builder{}
		whereCount := 0
		update.WriteString("UPDATE ")
		update.WriteString(db2.Esc(table))
		update.WriteString(" SET ")
		idx2 := 0
		for key, value := range values {
			value2 := db2.nullValue(value, structInfo[key])
			if structInfo[key].primaryKey {
				// skip primary keys for update
				if value2 == nil {
					return fmt.Errorf("Unable to build UPDATE clause with <nil> primary key: %s", key)
				}
				if whereCount > 0 {
					where.WriteString(" AND ")
				}
				where.WriteString(db2.Esc(key))
				where.WriteRune('=')
				where.WriteString(db2.escValueForInsert(value2, structInfo[key]))
				whereCount++
			} else {
				if idx2 > 0 {
					update.WriteRune(',')
				}
				idx2++
				update.WriteString(db2.Esc(key))
				update.WriteRune('=')
				update.WriteString(db2.escValueForInsert(value2, structInfo[key]))
			}
		}
		update.WriteString(" WHERE ")
		update.Write([]byte(where.String()))
		update.WriteRune(';')
		update.WriteRune('\n')
	}

	rowsAffected, _, err := db2.execContext(ctx, update.String())
	if err == nil && rowsAffected != 1 {
		err = ErrMismatchedRowsAffected
	}
	if err != nil {
		return db2.sqlError(err, update.String(), []any{})
	}

	return nil
}

func (db2 *db) insertStruct(ctx context.Context, table string, row any) (int64, structInfo, error) {
	values, info, err := db2.valuesFromStruct(row)
	if err != nil {
		return 0, nil, err
	}

	sql, args, err := db2.insertClauseFromValues(table, values, info)
	if err != nil {
		return 0, nil, err
	}

	if db2.UseReturningForLastId {
		pk := info.onlyPrimaryKey()
		if pk != nil {
			// Fail if transaction present and not in write mode
			if db2.sqlTx != nil && !db2.txWriteMode {
				return 0, nil, fmt.Errorf("[%s] Trying to write into read-only transaction: %s", db2, sql)
			}

			sql = sql + " RETURNING " + db2.Esc(pk.dbName)
			var insert_id_any any
			if db2.Debug || db2.DebugExec {
				log.Printf("%s SQL: %s\nARGS:\n%s", db2, sql, argsToString(args...))
			}
			err := db2.QueryContext(ctx, &insert_id_any, sql, args...)
			if err != nil {
				return 0, nil, err
			}
			insert_id, _ := insert_id_any.(int64) // ignore conversion error, return 0 in that case
			// log.Printf("Returning ID: %T %v", insert_id_any, insert_id_any)
			return insert_id, info, nil
		}
	}

	// log.Printf("SQL: %s Debug: %v", sql, db.Debug)
	rowsAffected, insert_id, err := db2.execContext(ctx, sql, args...)
	if err == nil && rowsAffected != 1 {
		err = ErrMismatchedRowsAffected
	}
	if err != nil {
		return 0, nil, err
	}

	return insert_id, info, nil
}

// clauseCacheKey identifies one generated INSERT/UPDATE SQL text. The text
// depends only on the table, the set of columns present (omitempty can vary
// the set per row), the row's Go type (pk designation partitions UPDATE into
// SET/WHERE) and the configured placeholder rune. Values/args vary per row,
// the SQL string does not.
type clauseCacheKey struct {
	typ         reflect.Type
	table       string
	cols        string // \x00-joined, sorted db column names present in values
	placeholder rune
}

// insertClauseCache / updateClauseCache memoize the generated SQL texts.
// Deterministic (sorted) column order makes the SQL stable, which lets pgx's
// statement cache (keyed on the exact SQL text) reuse one prepared statement
// instead of one per map-iteration order.
//
// The key space is bounded by (tables x types x present-column sets); omitempty
// makes the column set data-dependent, so the caches stop growing past
// clauseCacheMax entries each — later misses still work, they just rebuild.
var insertClauseCache, updateClauseCache sync.Map // clauseCacheKey -> string
var insertClauseCacheSize, updateClauseCacheSize atomic.Int64

const clauseCacheMax = 16384

func clauseCacheStore(cache *sync.Map, size *atomic.Int64, key clauseCacheKey, sqlS string) {
	if size.Load() >= clauseCacheMax {
		return
	}
	if _, loaded := cache.LoadOrStore(key, sqlS); !loaded {
		size.Add(1)
	}
}

func (db2 *db) insertClauseFromValues(table string, values map[string]any, info structInfo) (string, []any, error) {
	cols := slices.Sorted(maps.Keys(values))

	args := make([]any, 0, len(cols))
	for _, col := range cols {
		args = append(args, db2.nullValue(values[col], info[col]))
	}

	key := clauseCacheKey{table: table, cols: strings.Join(cols, "\x00")}
	if cached, ok := insertClauseCache.Load(key); ok {
		return cached.(string), args, nil
	}

	escCols := make([]string, len(cols))
	vs := make([]string, len(cols))
	for i, col := range cols {
		escCols[i] = db2.Esc(col)
		vs[i] = "?"
	}
	sqlS := fmt.Sprintf("INSERT INTO %s (%s) VALUES(%s)",
		db2.Esc(table),
		strings.Join(escCols, ","),
		strings.Join(vs, ","),
	)
	clauseCacheStore(&insertClauseCache, &insertClauseCacheSize, key, sqlS)
	return sqlS, args, nil
}

func (db2 *db) updateClauseFromRow(table string, row any) (string, []any, error) {

	var (
		args      []any
		whereArgs []any
		pk_value  any
	)

	values, structInfo, err := db2.valuesFromStruct(row)
	if err != nil {
		return "", nil, err
	}

	cols := slices.Sorted(maps.Keys(values))

	for _, key := range cols {
		value := values[key]
		if structInfo.primaryKey(key) {
			// skip primary keys for update
			pk_value = db2.nullValue(value, structInfo[key])
			if pk_value == nil {
				return "", args, fmt.Errorf("Unable to build UPDATE clause with <nil> key: %s", key)
			}
			whereArgs = append(whereArgs, pk_value)
		} else {
			args = append(args, db2.nullValue(value, structInfo[key]))
		}
	}

	if len(whereArgs) == 0 {
		return "", args, fmt.Errorf("Unable to build UPDATE clause, at least one key needed.")
	}

	args = append(args, whereArgs...)

	ck := clauseCacheKey{
		typ:         reflect.TypeOf(row),
		table:       table,
		cols:        strings.Join(cols, "\x00"),
		placeholder: db2.PlaceholderValue,
	}
	if cached, ok := updateClauseCache.Load(ck); ok {
		return cached.(string), args, nil
	}

	update := strings.Builder{}
	where := strings.Builder{}

	update.WriteString("UPDATE ")
	update.WriteString(db2.Esc(table))
	update.WriteString(" SET ")

	where.WriteString(" WHERE ")

	nSet, nWhere := 0, 0
	for _, key := range cols {
		if structInfo.primaryKey(key) {
			if nWhere > 0 {
				where.WriteString(" AND ")
			}
			where.WriteString(db2.Esc(key))
			where.WriteString("=")
			where.WriteRune(db2.PlaceholderValue)
			nWhere++
		} else {
			if nSet > 0 {
				update.WriteString(",")
			}
			update.WriteString(db2.Esc(key))
			update.WriteString("=")
			update.WriteRune(db2.PlaceholderValue)
			nSet++
		}
	}

	// Add where clause
	sqlS := update.String() + where.String()
	clauseCacheStore(&updateClauseCache, &updateClauseCacheSize, ck, sqlS)
	return sqlS, args, nil
}

func (db2 *db) Update(table string, data any) error {
	return db2.UpdateContext(context.Background(), table, data)
}

// Update updates the given struct or slice of structs
// The WHERE clause is put together from the "pk" columns.
// If not all "pk" columns have non empty values, Update returns
// an error.
func (db2 *db) UpdateContext(ctx context.Context, table string, data any) error {
	var (
		rv         reflect.Value
		structMode bool
		err        error
		update     string
		args       []any
	)

	if db2 == nil {
		panic("Update on <nil> handle.")
	}

	rv, structMode, err = checkData(data)
	if err != nil {
		return err
	}

	if structMode {
		update, args, err = db2.updateClauseFromRow(table, rv.Interface())
		if err != nil {
			return err
		}
		rowsAffected, _, err := db2.execContext(ctx, update, args...)
		if err == nil && rowsAffected != 1 {
			err = ErrMismatchedRowsAffected
		}
		if err != nil {
			return err
		}
	} else {
		for i := 0; i < rv.Len(); i++ {
			row := reflect.Indirect(rv.Index(i))
			update, args, err = db2.updateClauseFromRow(table, row.Interface())
			if err != nil {
				return err
			}
			rowsAffected, _, err := db2.execContext(ctx, update, args...)
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

func (db2 *db) Save(table string, data any) error {
	return db2.SaveContext(context.Background(), table, data)
}

// Save saves the given data. It performs an INSERT if the only primary key is
// zero, and and UPDATE if it is not. It panics if it the record has no primary
// key or less than one
func (db2 *db) SaveContext(ctx context.Context, table string, data any) error {

	rv, structMode, err := checkData(data)
	if err != nil {
		return err
	}

	if structMode {
		return db2.saveRow(ctx, table, data)
	} else {
		for i := 0; i < rv.Len(); i++ {
			err = db2.saveRow(ctx, table, rv.Index(i).Interface())
			if err != nil {
				return err
			}
		}
	}

	return nil
}

func (db2 *db) saveRow(ctx context.Context, table string, data any) error {
	row := reflect.Indirect(reflect.ValueOf(data))

	values, info, err := db2.valuesFromStruct(row.Interface())
	if err != nil {
		return err
	}
	pk := info.onlyPrimaryKey()

	if pk == nil {
		return fmt.Errorf("Save needs a struct with exactly one 'pk' field.")
	}

	pk_value, ok := values[pk.dbName]

	if !ok || isZero(pk_value) {
		return db2.InsertContext(ctx, table, data)
	} else {
		return db2.UpdateContext(ctx, table, data)
	}
}

// valuesFromStruct returns the relevant values
// from struct, as map
func (db2 *db) valuesFromStruct(data any) (map[string]any, structInfo, error) {
	var (
		info   structInfo
		values map[string]any
		dataV  reflect.Value
		err    error
	)

	values = make(map[string]any, 0)
	dataV = reflect.ValueOf(data)

	info = getStructInfo(dataV.Type())

	for _, fieldInfo := range info {
		dataF := dataV.FieldByIndex(fieldInfo.structField.Index)

		actualData := dataF.Interface()
		isZero := isZeroValue(dataF)

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
				if !fieldInfo.jsonIgnoreError {
					return nil, nil, errors.Wrap(err, "Unable to marshal as data as json.")
				}
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
func isZero(x any) bool {
	if x == nil {
		return true
	}
	return isZeroValue(reflect.ValueOf(x))
}

// isZeroValue reports whether v equals the zero value of its type. It
// replaces the former reflect.DeepEqual(x, boxed zero) check without boxing
// a zero value per call. Interface values are unwrapped so that an interface
// holding a typed nil pointer still counts as zero (DeepEqual semantics).
func isZeroValue(v reflect.Value) bool {
	for v.Kind() == reflect.Interface {
		if v.IsNil() {
			return true
		}
		v = v.Elem()
	}
	if !v.IsValid() {
		return true
	}
	return v.IsZero()
}

// execContext wraps DB.Exec and returns the number of affected rows as reported
// by the driver as well as the ID inserted, if the driver supports it.
func (db2 *db) execContext(ctx context.Context, execSql string, args ...any) (rowsAffected, insertID int64, err error) {
	var (
		execSql0 string
		newArgs  []any
	)

	if db2.txExecQueryMtx != nil {
		db2.txExecQueryMtx.Lock()
		defer db2.txExecQueryMtx.Unlock()
	}

	if db2.Debug || db2.DebugExec {
		log.Printf("%s SQL: %s\nARGS:\n%s", db2, execSql, argsToString(args...))
	}

	// Fail if transaction present and not in write mode
	if db2.sqlTx != nil && !db2.txWriteMode {
		return 0, 0, fmt.Errorf("[%s] Trying to write into read-only transaction: %s", db2, execSql)
	}

	if len(args) > 0 {
		execSql0, newArgs, err = db2.replaceArgs(execSql, args...)
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
		result, err = db2.db.ExecContext(ctx, execSql0, newArgs...)
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
			return 0, 0, db2.debugError(db2.sqlError(err, execSql0, newArgs))
		}
		break
	}

	row_count, err := func() (n int64, err error) {
		defer func() {
			if err != nil {
				return
			}
			// check if we have a panic (sqlite panics with empty execSql0 here)
			pnc := recover()
			if pnc != nil {
				err = fmt.Errorf("%v", pnc)
			}
		}()
		return result.RowsAffected()
	}()

	if !db2.SupportsLastInsertId || err != nil {
		return row_count, 0, nil
	}

	last_insert_id, err := result.LastInsertId()
	if err != nil {
		return row_count, 0, db2.debugError(err)
	}
	return row_count, last_insert_id, nil
}
