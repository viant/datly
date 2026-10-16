package validation

import (
	"fmt"
	"reflect"
	"strings"

	"github.com/viant/datly/constant"
	"github.com/viant/datly/constant/identifier"
	"github.com/viant/sqlx/io"
)

type planKey struct {
	typ       reflect.Type
	constants string
}

// SQLX owns constraint evaluation and derives its checks from a reflected type.
// Only when identifier tags change, give it a private, shallow column projection.
// Go validation, public methods and writer identity keep the original contract.
func (p *plan) resolveSQLShape(typ reflect.Type, values *constant.Values) error {
	p.sqlType = typ
	needed := false
	for _, column := range p.columns {
		if tag := column.Tag(); tag != nil && strings.Contains(tag.Table+tag.Db+tag.RefTable+tag.RefDb, "$") {
			needed = true
			break
		}
	}
	if !needed {
		return nil
	}
	renderer := identifier.Renderer{Values: values}
	var fields []reflect.StructField
	var indexes [][]int
	changed := false
	names := map[string]bool{}
	for _, column := range p.columns {
		fielder, ok := column.(io.Fielder)
		if !ok {
			return fmt.Errorf("validation column %s has no Go field", column.Name())
		}
		current := typ
		var index []int
		var field reflect.StructField
		for _, part := range fielder.Fields() {
			for current.Kind() == reflect.Pointer {
				current = current.Elem()
			}
			var found bool
			field, found = current.FieldByName(part.Name)
			if !found {
				return fmt.Errorf("validation field %s is not in %s", part.Name, current)
			}
			index = append(index, field.Index...)
			current = field.Type
		}
		if field.Name == "" || field.PkgPath != "" {
			return fmt.Errorf("SQL validation column %s requires an exported Go field", column.Name())
		}
		tag, err := renderer.Tag(field.Tag)
		if err != nil {
			return fmt.Errorf("validation field %s: %w", field.Name, err)
		}
		changed = changed || tag != field.Tag
		if names[field.Name] {
			return fmt.Errorf("ambiguous SQL validation field %s", field.Name)
		}
		names[field.Name] = true
		fields = append(fields, reflect.StructField{Name: field.Name, Type: field.Type, Tag: tag})
		indexes = append(indexes, index)
	}
	if changed {
		p.sqlType = reflect.StructOf(fields)
		p.sqlIndexes = indexes
	}
	return nil
}

func (p *plan) sqlValue(value any) any {
	if value == nil || len(p.sqlIndexes) == 0 {
		return value
	}
	row := reflect.ValueOf(value)
	for row.Kind() == reflect.Pointer {
		if row.IsNil() {
			return reflect.Zero(reflect.PointerTo(p.sqlType)).Interface()
		}
		row = row.Elem()
	}
	result := reflect.New(p.sqlType)
	for i, index := range p.sqlIndexes {
		field, err := row.FieldByIndexErr(index)
		if err == nil {
			result.Elem().Field(i).Set(field)
		}
	}
	return result.Interface()
}

func (p *plan) sqlBatch(at io.ValueAccessor, count int, original any) any {
	if len(p.sqlIndexes) == 0 {
		return original
	}
	result := reflect.MakeSlice(reflect.SliceOf(reflect.PointerTo(p.sqlType)), count, count)
	for i := 0; i < count; i++ {
		result.Index(i).Set(reflect.ValueOf(p.sqlValue(at(i))))
	}
	return result.Interface()
}
