package expand

import (
	"fmt"
	"github.com/viant/datly/internal/setter"
	"github.com/viant/datly/utils/types"
	"github.com/viant/sqlx/io"
	"github.com/viant/xunsafe"
	"math"
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
		Expand(source string, value interface{}) ([]*Expression, error)
		ColumnExpression(source string) string
	}

	Expression struct {
		ColumnExpression string
		SQLFragment      *strings.Builder
		Args             []interface{}
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

		if len(columns) > 63 {
			return nil, fmt.Errorf("unsupported criteria columns size (currently max 63)") //due to index implementation
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

		if aColumn != nil && aColumn.Name != "" {
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
	aTag := io.ParseTag(field.Tag)
	result.Name = aTag.Name()
	setter.SetStringIfEmpty(&result.Name, field.Name)
	result.Alias = aTag.Ns
	return result, nil
}

func (c *CriteriaExpander) HasAny(value interface{}) bool {
	if c.aSlice != nil {
		return c.aSlice.Len(xunsafe.AsPointer(value)) > 0
	}

	return xunsafe.AsPointer(value) != nil
}

func (c *CriteriaExpander) Expand(source string, value interface{}) ([]*Expression, error) {
	var expressions []*Expression
	if c.aSlice != nil {
		expressionsIndex := map[int64]int{}
		ptr := xunsafe.AsPointer(value)

		size := c.aSlice.Len(ptr)
		for i := 0; i < size; i++ {
			index := c.itemIndex(c.aSlice.ValueAt(ptr, i))
			expressionIndex, ok := expressionsIndex[index]
			if !ok {
				expressionIndex = len(expressions)
				expressions = append(expressions, c.newExpression(source, index))
				expressionsIndex[index] = expressionIndex
			}
			expression := expressions[expressionIndex]

			if len(expression.Args) > 0 {
				expression.SQLFragment.WriteString(", ")
			}
			c.expandItem(expression.SQLFragment, &expression.Args, c.aSlice.ValueAt(ptr, i), index)
		}
	} else {
		itemIndex := c.itemIndex(value)
		expression := c.newExpression(source, itemIndex)
		c.expandItem(expression.SQLFragment, &expression.Args, value, itemIndex)
		expressions = append(expressions, expression)
	}

	return expressions, nil
}

func (c *CriteriaExpander) newExpression(source string, index int64) *Expression {
	return &Expression{
		ColumnExpression: c.columnExpression(source, index),
		SQLFragment:      &strings.Builder{},
		Args:             nil,
	}
}

func (c *CriteriaExpander) ColumnExpression(source string) string {
	return c.columnExpression(source, int64(math.MaxInt64))
}

func (c *CriteriaExpander) columnExpression(source string, presence int64) string {
	sb := &strings.Builder{}
	sb.WriteString("(")

	var i int
	for colIndex, column := range c.columns {
		if presence&(1<<colIndex) == 0 {
			continue
		}

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
		i++
	}

	sb.WriteString(" )")
	return sb.String()
}

func (c *CriteriaExpander) itemIndex(value interface{}) int64 {
	ptr := xunsafe.AsPointer(value)
	var result int64
	fmt.Printf("%+v\n", value)
	for i, column := range c.columns {
		fieldValue := reflect.ValueOf(column.Field.Value(ptr))
		if (fieldValue.Kind() == reflect.Ptr && !fieldValue.IsNil()) || !fieldValue.IsZero() {
			result |= 1 << i
		}
	}

	return result
}

func (c *CriteriaExpander) expandItem(sb *strings.Builder, bindings *[]interface{}, value interface{}, index int64) {
	ptr := xunsafe.AsPointer(value)
	sb.WriteString(" ( ")

	var i = 0
	for j, column := range c.columns {
		if index&(1<<j) == 0 {
			continue
		}

		if i != 0 {
			sb.WriteString(", ")
		}

		fieldValue := column.Field.Value(ptr)
		*bindings = append(*bindings, fieldValue)
		sb.WriteString(" ? ")
		i++
	}
	sb.WriteString(" ) ")
}

func (s *SliceExpander) HasAny(value interface{}) bool {
	ptr := xunsafe.AsPointer(value)
	return s.aSlice.Len(ptr) > 0
}

func (s *SliceExpander) Expand(source string, value interface{}) ([]*Expression, error) {
	ptr := xunsafe.AsPointer(value)
	size := s.aSlice.Len(ptr)
	sb := &strings.Builder{}
	result := make([]interface{}, 0, size)

	for i := 0; i < size; i++ {
		if i != 0 {
			sb.WriteString(", ")
		}

		expanded, err := s.itemExpander.Expand(source, s.aSlice.ValueAt(ptr, i))
		if err != nil {
			return nil, err
		}

		for _, expression := range expanded {
			result = append(result, expression.Args...)
			sb.WriteString(expression.SQLFragment.String())

		}
	}

	return NewSingleExpression(s.ColumnExpression(source), sb.String(), result), nil
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

func (p *PrimitiveExpander) Expand(source string, value interface{}) ([]*Expression, error) {
	switch actual := value.(type) {
	case *string:
		return NewSingleExpression(p.ColumnExpression(source), "?", []interface{}{actual}), nil
	case *int:
		return NewSingleExpression(p.ColumnExpression(source), "?", []interface{}{actual}), nil
	case *int64:
		return NewSingleExpression(p.ColumnExpression(source), "?", []interface{}{actual}), nil
	case *uint64:
		return NewSingleExpression(p.ColumnExpression(source), "?", []interface{}{actual}), nil
	case *float32:
		return NewSingleExpression(p.ColumnExpression(source), "?", []interface{}{actual}), nil
	case *float64:
		return NewSingleExpression(p.ColumnExpression(source), "?", []interface{}{actual}), nil
	case *uint:
		return NewSingleExpression(p.ColumnExpression(source), "?", []interface{}{actual}), nil
	case *bool:
		return NewSingleExpression(p.ColumnExpression(source), "?", []interface{}{actual}), nil
	case *int8:
		return NewSingleExpression(p.ColumnExpression(source), "?", []interface{}{actual}), nil
	case *uint8:
		return NewSingleExpression(p.ColumnExpression(source), "?", []interface{}{actual}), nil
	case *int32:
		return NewSingleExpression(p.ColumnExpression(source), "?", []interface{}{actual}), nil
	case *uint32:
		return NewSingleExpression(p.ColumnExpression(source), "?", []interface{}{actual}), nil
	case *int16:
		return NewSingleExpression(p.ColumnExpression(source), "?", []interface{}{actual}), nil
	case *uint16:
		return NewSingleExpression(p.ColumnExpression(source), "?", []interface{}{actual}), nil
	}

	srcValue := reflect.ValueOf(value)
	dstValue := reflect.New(srcValue.Type())
	dstValue.Elem().Set(srcValue)
	valueCopy := dstValue.Elem().Interface()
	return NewSingleExpression(p.ColumnExpression(source), "?", []interface{}{valueCopy}), nil
}

func NewSingleExpression(expression string, fragment string, args []interface{}) []*Expression {
	builder := &strings.Builder{}
	builder.WriteString(fragment)
	return []*Expression{{ColumnExpression: expression, Args: args, SQLFragment: builder}}
}
