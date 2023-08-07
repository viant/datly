package expand

import (
	"fmt"
	"github.com/viant/datly/utils/types"
	"github.com/viant/xunsafe"
	"reflect"
	"strings"
	"sync"
)

var bindingsCache = &BindingsCache{
	registry: sync.Map{},
}

type (
	BindingsCache struct {
		registry sync.Map
	}

	BindingsExpander interface {
		HasAny(value interface{}) bool
		Expand(source string, value interface{}) (string, []interface{}, error)
		ColumnExpression(source string) string
	}

	PrimitiveExpander struct {
	}

	SliceExpander struct {
		aSlice       *xunsafe.Slice
		itemExpander BindingsExpander
	}

	CriteriaExpander struct {
		columns []*CriteriaColumn
		aSlice  *xunsafe.Slice
	}

	CriteriaColumn struct {
		Name  string
		Alias string
		Field *xunsafe.Field
	}
)

func (b *BindingsCache) Lookup(value interface{}) (BindingsExpander, error) {
	rType := reflect.TypeOf(value)
	if loaded, ok := b.registry.Load(rType); ok {
		return loaded.(BindingsExpander), nil
	}

	expander, err := b.getBindingsExpander(rType)
	if err != nil {
		return nil, err
	}

	b.registry.Store(rType, expander)
	return expander, nil
}

func (b *BindingsCache) getBindingsExpander(rType reflect.Type) (BindingsExpander, error) {
	elem := types.Elem(rType)
	columns, err := b.extractCriteriaColumns(elem)
	if err != nil {
		return nil, err
	}

	if len(columns) > 0 {
		var aSlice *xunsafe.Slice
		if rType.Kind() == reflect.Slice || rType.Kind() == reflect.Array {
			aSlice = xunsafe.NewSlice(rType)
		}

		return &CriteriaExpander{
			columns: columns,
			aSlice:  aSlice,
		}, nil
	}

	switch rType.Kind() {
	case reflect.Slice, reflect.Array:
		return &SliceExpander{
			aSlice:       xunsafe.NewSlice(rType),
			itemExpander: &PrimitiveExpander{},
		}, err
	}

	return &PrimitiveExpander{}, nil
}

func (b *BindingsCache) extractCriteriaColumns(elem reflect.Type) ([]*CriteriaColumn, error) {
	if elem.Kind() != reflect.Struct {
		return nil, nil
	}

	var columns []*CriteriaColumn
	numField := elem.NumField()
	for i := 0; i < numField; i++ {
		field := elem.Field(i)
		aColumn, err := ParseDatlyTag(field)
		if err != nil {
			return nil, err
		}

		if aColumn != nil {
			columns = append(columns, aColumn)
		}
	}

	return columns, nil
}

func ParseDatlyTag(field reflect.StructField) (*CriteriaColumn, error) {
	tagContent := field.Tag.Get("sqlx")
	if tagContent == "" {
		return nil, nil
	}

	result := &CriteriaColumn{
		Field: xunsafe.NewField(field),
	}

	segments := strings.Split(tagContent, "|")
	for _, segment := range segments {
		split := strings.Split(segment, "=")
		if len(split) != 2 {
			return nil, fmt.Errorf("criteria tag must be format of name=value")
		}

		key := strings.ToLower(split[0])
		switch key {
		case "column":
			result.Name = split[1]
		case "alias":
			result.Alias = split[1]
		}
	}

	return result, nil
}

func (c *CriteriaExpander) HasAny(value interface{}) bool {
	if c.aSlice != nil {
		return c.aSlice.Len(xunsafe.AsPointer(value)) > 0
	}

	return xunsafe.AsPointer(value) != nil
}

func (c *CriteriaExpander) Expand(source string, value interface{}) (string, []interface{}, error) {
	sb := &strings.Builder{}
	var bindings []interface{}
	if c.aSlice != nil {
		ptr := xunsafe.AsPointer(value)
		size := c.aSlice.Len(ptr)
		for i := 0; i < size; i++ {
			if i != 0 {
				sb.WriteString(", ")
			}
			c.expandItem(sb, &bindings, c.aSlice.ValueAt(ptr, i))
		}
	} else {
		c.expandItem(sb, &bindings, value)
	}

	return sb.String(), bindings, nil
}

func (c *CriteriaExpander) ColumnExpression(source string) string {
	sb := &strings.Builder{}
	sb.WriteString("(")
	for i, column := range c.columns {
		if i != 0 {
			sb.WriteString(", ")
		}

		if column.Alias != "" {
			sb.WriteString(column.Alias)
		} else {
			sb.WriteString(source)
		}

		sb.WriteString(".")
		sb.WriteString(column.Name)
	}

	sb.WriteString(" )")
	return sb.String()
}

func (c *CriteriaExpander) expandItem(sb *strings.Builder, bindings *[]interface{}, value interface{}) {
	ptr := xunsafe.AsPointer(value)
	sb.WriteString(" ( ")
	for i, column := range c.columns {
		if i != 0 {
			sb.WriteString(", ")
		}

		fieldValue := column.Field.Value(ptr)
		*bindings = append(*bindings, fieldValue)
		sb.WriteString(" ? ")
	}
	sb.WriteString(" ) ")
}

func (s *SliceExpander) HasAny(value interface{}) bool {
	ptr := xunsafe.AsPointer(value)
	return s.aSlice.Len(ptr) > 0
}

func (s *SliceExpander) Expand(source string, value interface{}) (string, []interface{}, error) {
	ptr := xunsafe.AsPointer(value)
	size := s.aSlice.Len(ptr)
	sb := &strings.Builder{}
	result := make([]interface{}, 0, size)
	for i := 0; i < size; i++ {
		if i != 0 {
			sb.WriteString(", ")
		}

		expanded, bindings, err := s.itemExpander.Expand(source, s.aSlice.ValueAt(ptr, i))
		if err != nil {
			return "", nil, err
		}

		result = append(result, bindings...)
		sb.WriteString(expanded)
	}

	return sb.String(), result, nil
}

func (s *SliceExpander) ColumnExpression(source string) string {
	return source
}

func (p *PrimitiveExpander) ColumnExpression(source string) string {
	return source
}

func (p *PrimitiveExpander) HasAny(value interface{}) bool {
	return true
}

func (p *PrimitiveExpander) Expand(source string, value interface{}) (string, []interface{}, error) {
	switch actual := value.(type) {
	case *string:
		return "?", []interface{}{actual}, nil
	case *int:
		return "?", []interface{}{actual}, nil
	case *int64:
		return "?", []interface{}{actual}, nil
	case *uint64:
		return "?", []interface{}{actual}, nil
	case *float32:
		return "?", []interface{}{actual}, nil
	case *float64:
		return "?", []interface{}{actual}, nil
	case *uint:
		return "?", []interface{}{actual}, nil
	case *bool:
		return "?", []interface{}{actual}, nil
	case *int8:
		return "?", []interface{}{actual}, nil
	case *uint8:
		return "?", []interface{}{actual}, nil
	case *int32:
		return "?", []interface{}{actual}, nil
	case *uint32:
		return "?", []interface{}{actual}, nil
	case *int16:
		return "?", []interface{}{actual}, nil
	case *uint16:
		return "?", []interface{}{actual}, nil
	}

	srcValue := reflect.ValueOf(value)
	dstValue := reflect.New(srcValue.Type())
	dstValue.Elem().Set(srcValue)
	valueCopy := dstValue.Elem().Interface()

	return "?", []interface{}{valueCopy}, nil
}
