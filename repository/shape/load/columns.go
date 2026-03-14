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
		if shouldSkipInferredField(f) {
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

func shouldSkipInferredField(field reflect.StructField) bool {
	if field.Name == "-" {
		return true
	}
	rawTag := string(field.Tag)
	if strings.Contains(rawTag, `view:"`) || strings.Contains(rawTag, `on:"`) {
		return true
	}
	if strings.Contains(rawTag, `sqlx:"-"`) {
		return true
	}
	return false
}

func inferredColumnsArePlaceholders(columns []*view.Column) bool {
	if len(columns) == 0 {
		return false
	}
	for _, column := range columns {
		if column == nil || !isPlaceholderColumnName(column.Name) {
			return false
		}
	}
	return true
}

func isPlaceholderColumnName(name string) bool {
	name = strings.TrimSpace(strings.ToLower(name))
	name = strings.ReplaceAll(name, "_", "")
	if !strings.HasPrefix(name, "col") || len(name) == len("col") {
		return false
	}
	for i := len("col"); i < len(name); i++ {
		if name[i] < '0' || name[i] > '9' {
			return false
		}
	}
	return true
}

// sqlxColumnName reads the sqlx struct tag to get the database column name.
func sqlxColumnName(f reflect.StructField) string {
	tag := f.Tag.Get("sqlx")
	if tag == "" {
		return ""
	}
	for _, part := range strings.Split(tag, ",") {
		part = strings.TrimSpace(part)
		if part == "" {
			continue
		}
		if strings.HasPrefix(part, "name=") {
			return strings.TrimPrefix(part, "name=")
		}
		if !strings.Contains(part, "=") {
			return part
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
	case reflect.Map:
		return "map[" + reflectDataType(t.Key()) + "]" + reflectDataType(t.Elem())
	case reflect.Interface:
		return "interface{}"
	default:
		return "string"
	}
}
