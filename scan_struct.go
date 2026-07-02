package sqlpro

import (
	"database/sql"
	"encoding/json"
	"reflect"
	"time"

	"github.com/pkg/errors"
)

var (
	typeTime       = reflect.TypeOf(time.Time{})
	typeRawMessage = reflect.TypeOf(json.RawMessage(nil))
	sharedVoidScan = &voidScan{}
)

// scanKind classifies how a result column maps onto a struct field. It is
// derived once from the field's static type so the per-row loop needs no
// type discovery.
type scanKind uint8

const (
	kindSkip   scanKind = iota // column not mapped to any field
	kindDirect                 // scan straight into the field's address
	kindString
	kindInt64
	kindFloat64
	kindBool
	kindTime
	kindJson // db:",json" tag
	kindRaw  // json.RawMessage
)

// colPlan is the precomputed scan plan for one result column against a struct
// row type. It is built once per Scan() (slice-of-struct mode) and reused for
// every row, so the per-row work does no FieldByName lookup, no type discovery,
// and (for the null-scanner kinds) no per-row allocation.
type colPlan struct {
	kind            scanKind
	index           []int // struct field index path, for FieldByIndex
	ptr             bool  // the field is a pointer
	jsonIgnoreError bool
	scanner         any // reused null-scanner instance (kindString..kindRaw)
}

func isUintKind(k reflect.Kind) bool {
	switch k {
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
		return true
	}
	return false
}

func isIntKind(k reflect.Kind) bool {
	switch k {
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		return true
	}
	return isUintKind(k)
}

// buildColPlan maps each result column to a struct field and the cheapest way
// to scan it. Each null-scanner kind gets one reused scanner instance.
func buildColPlan(cols []string, info structInfo) []colPlan {
	plan := make([]colPlan, len(cols))
	for i, col := range cols {
		finfo, ok := info[col]
		if !ok {
			plan[i].kind = kindSkip
			continue
		}
		p := &plan[i]
		p.index = finfo.structField.Index
		p.jsonIgnoreError = finfo.jsonIgnoreError

		ft := finfo.structField.Type
		p.ptr = ft.Kind() == reflect.Ptr
		et := ft
		if p.ptr {
			et = et.Elem()
		}

		switch {
		case finfo.isJson:
			p.kind = kindJson
			p.scanner = &NullJson{}
		case ft == typeRawMessage || et == typeRawMessage:
			p.kind = kindRaw
			p.scanner = &NullRawMessage{}
		case et.Kind() == reflect.String:
			p.kind = kindString
			p.scanner = &sql.NullString{}
		case isIntKind(et.Kind()):
			p.kind = kindInt64
			p.scanner = &sql.NullInt64{}
		case et.Kind() == reflect.Float32 || et.Kind() == reflect.Float64:
			p.kind = kindFloat64
			p.scanner = &sql.NullFloat64{}
		case et.Kind() == reflect.Bool:
			p.kind = kindBool
			p.scanner = &sql.NullBool{}
		case et == typeTime:
			p.kind = kindTime
			p.scanner = &NullTime{}
		default:
			p.kind = kindDirect
		}
	}
	return plan
}

// resetScanner clears the two custom scanners whose Scan() does not reset on a
// NULL/empty value; reusing them across rows would otherwise leak the previous
// row's value. The stdlib sql.Null* scanners and NullTime reset themselves.
func resetScanner(p *colPlan) {
	switch p.kind {
	case kindJson:
		s := p.scanner.(*NullJson)
		s.Valid, s.Data = false, nil
	case kindRaw:
		s := p.scanner.(*NullRawMessage)
		s.Valid, s.Data = false, nil
	}
}

// readbackCol copies one reused scanner's value into the struct field. It always
// copies by value (never captures the address of a reused scanner's field), so
// the next row's Scan cannot corrupt an already-stored value.
func readbackCol(fieldV reflect.Value, p *colPlan) error {
	switch p.kind {
	case kindString:
		s := p.scanner.(*sql.NullString)
		if p.ptr {
			if s.Valid {
				nv := reflect.New(fieldV.Type().Elem())
				nv.Elem().SetString(s.String)
				fieldV.Set(nv)
			} else {
				fieldV.SetZero()
			}
		} else {
			fieldV.SetString(s.String)
		}
	case kindInt64:
		s := p.scanner.(*sql.NullInt64)
		if p.ptr {
			if s.Valid {
				nv := reflect.New(fieldV.Type().Elem())
				if isUintKind(nv.Elem().Kind()) {
					nv.Elem().SetUint(uint64(s.Int64))
				} else {
					nv.Elem().SetInt(s.Int64)
				}
				fieldV.Set(nv)
			} else {
				fieldV.SetZero()
			}
		} else if isUintKind(fieldV.Kind()) {
			fieldV.SetUint(uint64(s.Int64))
		} else {
			fieldV.SetInt(s.Int64)
		}
	case kindFloat64:
		s := p.scanner.(*sql.NullFloat64)
		if p.ptr {
			if s.Valid {
				nv := reflect.New(fieldV.Type().Elem())
				nv.Elem().SetFloat(s.Float64)
				fieldV.Set(nv)
			} else {
				fieldV.SetZero()
			}
		} else {
			fieldV.SetFloat(s.Float64)
		}
	case kindBool:
		s := p.scanner.(*sql.NullBool)
		if p.ptr {
			if s.Valid {
				nv := reflect.New(fieldV.Type().Elem())
				nv.Elem().SetBool(s.Bool)
				fieldV.Set(nv)
			} else {
				fieldV.SetZero()
			}
		} else {
			fieldV.SetBool(s.Bool)
		}
	case kindTime:
		s := p.scanner.(*NullTime)
		if p.ptr {
			if s.Valid {
				nv := reflect.New(typeTime)
				nv.Elem().Set(reflect.ValueOf(s.Time))
				fieldV.Set(nv)
			} else {
				fieldV.SetZero()
			}
		} else if s.Valid {
			fieldV.Set(reflect.ValueOf(s.Time))
		} else {
			fieldV.SetZero()
		}
	case kindRaw:
		s := p.scanner.(*NullRawMessage)
		if s.Valid {
			d := append([]byte(nil), s.Data...) // copy: scanner data is reused/reset
			if p.ptr {
				nv := reflect.New(fieldV.Type().Elem())
				nv.Elem().SetBytes(d)
				fieldV.Set(nv)
			} else {
				fieldV.SetBytes(d)
			}
		} else {
			fieldV.SetZero()
		}
	case kindJson:
		s := p.scanner.(*NullJson)
		if s.Valid {
			nv := reflect.New(fieldV.Type())
			if err := json.Unmarshal(s.Data, nv.Interface()); err != nil {
				if !p.jsonIgnoreError {
					return errors.Wrapf(err, "Error unmarshalling data: %q", string(s.Data))
				}
			}
			fieldV.Set(nv.Elem())
		} else {
			fieldV.SetZero()
		}
	}
	return nil
}

// scalarScanKind classifies a slice element type for the scalar-slice fast
// path. It matches only the predeclared scalar types (and time.Time), exactly
// like the concrete-type switches in scanRow: named types such as
// "type ID int64" keep using the generic path (direct scan, no null-scanner).
// ok is false for types the fast path does not handle.
func scalarScanKind(baseType reflect.Type, isPtr bool) (kind scanKind, ok bool) {
	if baseType == typeTime {
		// []*time.Time keeps the generic path: it scans through the
		// dereferenced struct, so a NULL yields a non-nil pointer to a zero
		// time there — the fast path would yield nil instead.
		return kindTime, !isPtr
	}
	if baseType.PkgPath() != "" {
		return 0, false
	}
	switch baseType.Kind() {
	case reflect.String:
		return kindString, true
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64,
		reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
		return kindInt64, true
	case reflect.Float32, reflect.Float64:
		return kindFloat64, true
	case reflect.Bool:
		return kindBool, true
	}
	return 0, false
}

// scanScalarSlice scans the first column of every row into target, a slice of
// predeclared scalar values ([]int64, []string, []*int64, []time.Time, ...).
// The null-scanner and the scan-destination buffer are allocated once and
// reused for every row; columns past the first are ignored, as in scanRow.
// This is the fast path for the common "SELECT id FROM ..." bulk query.
func scanScalarSlice(target reflect.Value, elemType reflect.Type, elemIsPtr bool, kind scanKind, rows *sql.Rows) error {
	cols, err := rows.Columns()
	if err != nil {
		return err
	}

	var scanner any
	switch kind {
	case kindString:
		scanner = &sql.NullString{}
	case kindInt64:
		scanner = &sql.NullInt64{}
	case kindFloat64:
		scanner = &sql.NullFloat64{}
	case kindBool:
		scanner = &sql.NullBool{}
	case kindTime:
		scanner = &NullTime{}
	}

	// Only the first column maps onto the value, the rest is discarded.
	data := make([]any, len(cols))
	if len(cols) > 0 {
		data[0] = scanner
	}
	for i := 1; i < len(cols); i++ {
		data[i] = sharedVoidScan
	}

	p := &colPlan{kind: kind, ptr: elemIsPtr, scanner: scanner}

	// Work on a local slice header and write it back to the target once (and
	// on error, so rows scanned before a failure stay visible, as in the
	// generic path).
	slice := target
	for rows.Next() {
		if err := rows.Scan(data...); err != nil {
			target.Set(slice)
			return err
		}
		slice = reflect.Append(slice, reflect.Zero(elemType))
		if len(cols) == 0 {
			continue
		}
		if err := readbackCol(slice.Index(slice.Len()-1), p); err != nil {
			target.Set(slice)
			return err
		}
	}
	target.Set(slice)
	return rows.Err()
}

// scanStructSlice scans every row into target, a slice of struct or *struct.
// The column plan, the scan-destination buffer and the null-scanners are built
// once and reused across all rows; only the new element itself is allocated per
// row. This is the fast path for the common []*Struct / []Struct query.
func scanStructSlice(target reflect.Value, structType reflect.Type, elemIsPtr bool, rows *sql.Rows) error {
	cols, err := rows.Columns()
	if err != nil {
		return err
	}

	plan := buildColPlan(cols, getStructInfo(structType))
	data := make([]any, len(cols))

	// chunk of structs handed out one-by-one for []*T targets: one allocation
	// per chunkSize rows instead of one reflect.New per row.
	const chunkSize = 64
	var chunk reflect.Value
	chunkNext := chunkSize // force initial allocation
	var structSliceType reflect.Type
	if elemIsPtr {
		structSliceType = reflect.SliceOf(structType)
	}

	for rows.Next() {
		// Grow the destination by one in place; Grow amortizes the backing
		// growth, SetLen exposes the new zeroed element.
		n := target.Len()
		target.Grow(1)
		target.SetLen(n + 1)
		elemV := target.Index(n)

		structV := elemV
		if elemIsPtr {
			if chunkNext == chunkSize {
				chunk = reflect.MakeSlice(structSliceType, chunkSize, chunkSize)
				chunkNext = 0
			}
			structV = chunk.Index(chunkNext)
			chunkNext++
			elemV.Set(structV.Addr())
		}

		// Bind scan destinations: reused scanners for null-scanner columns,
		// the field address itself for direct columns.
		for i := range plan {
			p := &plan[i]
			switch p.kind {
			case kindSkip:
				data[i] = sharedVoidScan
			case kindDirect:
				fieldV := structV.FieldByIndex(p.index)
				if p.ptr {
					if fieldV.IsNil() {
						fieldV.Set(reflect.New(fieldV.Type().Elem()))
					}
					data[i] = fieldV.Interface()
				} else {
					data[i] = fieldV.Addr().Interface()
				}
			default:
				resetScanner(p)
				data[i] = p.scanner
			}
		}

		if err := rows.Scan(data...); err != nil {
			return err
		}

		// Read back the null-scanner columns into the struct fields.
		for i := range plan {
			p := &plan[i]
			if p.kind == kindSkip || p.kind == kindDirect {
				continue
			}
			if err := readbackCol(structV.FieldByIndex(p.index), p); err != nil {
				return err
			}
		}
	}

	return rows.Err()
}
