package sqlpro

import (
	"fmt"
	"reflect"
	"strings"
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
