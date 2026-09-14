package fragment

import (
	"database/sql/driver"
	"fmt"
	"reflect"
	"strings"

	sqlxio "github.com/viant/sqlx/io"
	"github.com/viant/sqlx/option"
)

// CompositeIn renders a fixed tuple predicate from typed key records. SQLX
// owns column mapping and dialect syntax; all mapped fields participate even
// when their values are zero, false or null. Callers supply a key projection,
// not arbitrary mutable entity fields. Empty input is a no-match predicate.
func (c *Context) CompositeIn(alias string, rows any) (string, error) {
	if c == nil || c.bindings == nil {
		return "", fmt.Errorf("SQL fragment bindings are required")
	}
	value := reflect.ValueOf(rows)
	for value.IsValid() && (value.Kind() == reflect.Pointer || value.Kind() == reflect.Interface) {
		if value.IsNil() {
			return "1 = 0", nil
		}
		value = value.Elem()
	}
	if !value.IsValid() {
		return "1 = 0", nil
	}
	if value.Kind() != reflect.Slice && value.Kind() != reflect.Array {
		return "", fmt.Errorf("composite IN requires a typed key slice or array, got %T", rows)
	}
	if value.Len() == 0 {
		return "1 = 0", nil
	}
	recordType := value.Type().Elem()
	for recordType.Kind() == reflect.Pointer {
		recordType = recordType.Elem()
	}
	if recordType.Kind() != reflect.Struct {
		return "", fmt.Errorf("composite IN key must be a struct, got %s", recordType)
	}
	columns, binder, err := sqlxio.StructColumnMapper(recordType, option.StructOrderedColumns(true))
	if err != nil {
		return "", fmt.Errorf("map composite key columns: %w", err)
	}
	if len(columns) == 0 {
		return "", fmt.Errorf("composite IN key has no mapped columns")
	}
	names := make([]string, len(columns))
	seen := map[string]bool{}
	alias = strings.TrimSpace(alias)
	for index, column := range columns {
		name := strings.TrimSpace(column.Name())
		if name == "" {
			return "", fmt.Errorf("composite IN has an empty mapped column")
		}
		namespace := alias
		if tag := column.Tag(); tag != nil && tag.Ns != "" {
			namespace = tag.Ns
		}
		if namespace != "" {
			name = namespace + "." + name
		}
		if seen[name] {
			return "", fmt.Errorf("composite IN has duplicate mapped column %q", name)
		}
		seen[name] = true
		names[index] = name
	}
	var args []any
	for index := 0; index < value.Len(); index++ {
		row := value.Index(index)
		for row.Kind() == reflect.Pointer {
			if row.IsNil() {
				return "", fmt.Errorf("composite IN row %d is nil", index)
			}
			row = row.Elem()
		}
		if !row.CanAddr() {
			copy := reflect.New(recordType).Elem()
			copy.Set(row)
			row = copy
		}
		values := make([]any, len(columns))
		binder(row.Addr().Interface(), values, 0, len(columns))
		for field, item := range values {
			converted, err := driver.DefaultParameterConverter.ConvertValue(item)
			if err != nil {
				return "", fmt.Errorf("composite IN row %d column %s: %w", index, names[field], err)
			}
			args = append(args, converted)
		}
	}
	// Publish bindings only after every row has validated and converted.
	fragment := c.dialect.CompositeIn(names, value.Len())
	if strings.TrimSpace(fragment) == "" {
		return "", fmt.Errorf("composite IN dialect renderer returned empty SQL")
	}
	c.bindings.Append(args...)
	return fragment, nil
}
