package load

import (
	"reflect"
	"strings"

	"github.com/viant/datly/view"
)

var mapStringInterface = reflect.TypeOf(map[string]interface{}{})

// inferColumnsFromType extracts column descriptors from a statically-inferred struct type.
// Returns nil when rType is nil, non-struct, or the untyped map[string]interface{} fallback.
func inferColumnsFromType(rType reflect.Type) []*view.Column {
	if rType == nil {
		return nil
	}
	// Unwrap slice / pointer wrappers
	for rType.Kind() == reflect.Ptr || rType.Kind() == reflect.Slice {
		rType = rType.Elem()
	}
	if rType.Kind() != reflect.Struct {
		return nil
	}
	// Skip the untyped fallback used when columns are unknown
	if rType == mapStringInterface {
		return nil
	}
	cols := make([]*view.Column, 0, rType.NumField())
	for i := 0; i < rType.NumField(); i++ {
		f := rType.Field(i)
		if !f.IsExported() {
			continue
		}
		colName := sqlxColumnName(f)
		if colName == "" {
			colName = f.Name
		}
		cols = append(cols, &view.Column{
			Name:     colName,
			DataType: reflectDataType(f.Type),
		})
	}
	return cols
}

// sqlxColumnName reads the sqlx struct tag to get the database column name.
func sqlxColumnName(f reflect.StructField) string {
	tag := f.Tag.Get("sqlx")
	if tag == "" {
		return ""
	}
	for _, part := range strings.Split(tag, ",") {
		part = strings.TrimSpace(part)
		if strings.HasPrefix(part, "name=") {
			return strings.TrimPrefix(part, "name=")
		}
	}
	return ""
}

// reflectDataType maps a Go reflect.Type to a datly column DataType string.
func reflectDataType(t reflect.Type) string {
	for t.Kind() == reflect.Ptr {
		t = t.Elem()
	}
	switch t.Kind() {
	case reflect.String:
		return "string"
	case reflect.Bool:
		return "bool"
	case reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int:
		return "int"
	case reflect.Int64:
		return "int64"
	case reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint:
		return "int"
	case reflect.Uint64:
		return "int64"
	case reflect.Float32:
		return "float32"
	case reflect.Float64:
		return "float64"
	case reflect.Slice:
		if t.Elem().Kind() == reflect.Uint8 {
			return "[]byte"
		}
		return "[]" + reflectDataType(t.Elem())
	default:
		return "string"
	}
}
