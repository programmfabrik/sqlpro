package sqlpro

import (
	"reflect"
	"strings"
)

type structInfo map[string]fieldInfo

type fieldInfo struct {
	name       string
	dbName     string
	omitEmpty  bool
	primaryKey bool
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
			default:
				// ignore unrecognized
			}
		}
		si[info.dbName] = info
	}
	return si
}
