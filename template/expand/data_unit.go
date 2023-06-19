package expand

import (
	"context"
	"fmt"
	"github.com/google/uuid"
	"github.com/viant/datly/executor/sequencer"
	"github.com/viant/sqlx/io/validator"
	"github.com/viant/xunsafe"
	"reflect"
	"strings"
	"unsafe"
)

type (
	ColumnsSource interface {
		ColumnName(key string) (string, error)
	}

	DataUnit struct {
		Columns     ColumnsSource
		ParamsGroup []interface{}
		Mock        bool
		TemplateSQL string
		MetaSource  Dber
		Statements  *Statements `velty:"-"`

		placeholderCounter int                             `velty:"-"`
		sqlxValidator      *validator.Service              `velty:"-"`
		sliceIndex         map[reflect.Type]*xunsafe.Slice `velty:"-"`
	}

	ExecutablesIndex map[string]*Executable
)

func (c *DataUnit) WithPresence() interface{} {
	var opt interface{} = validator.WithSetMarker()
	return opt
}
func (c *DataUnit) WithLocation(loc string) interface{} {
	var opt interface{} = validator.WithLocation(loc)
	return opt
}

func (c *DataUnit) Validate(dest interface{}, opts ...interface{}) (*validator.Validation, error) {
	db, err := c.MetaSource.Db()
	if err != nil {
		fmt.Printf("error occured while connecting to DB %v\n", err.Error())
		return nil, fmt.Errorf("error occurred while connecting to DB")
	}
	if c.sqlxValidator == nil {
		c.sqlxValidator = validator.New()
	}
	var options []validator.Option
	for _, opt := range opts {
		if o, ok := (opt).(validator.Option); ok {
			options = append(options, o)
		}
	}
	return c.sqlxValidator.Validate(context.Background(), db, dest, options...)
}

func (c *DataUnit) Allocate(tableName string, dest interface{}, selector string) (string, error) {
	db, err := c.MetaSource.Db()
	if err != nil {
		fmt.Printf("error occured while connecting to DB %v\n", err.Error())

		return "", fmt.Errorf("error occurred while connecting to DB")
	}

	service := sequencer.New(context.Background(), db)
	return "", service.Next(tableName, dest, selector)
}

func (c *DataUnit) AsBinding(value interface{}) string {
	return c.Add(0, value)
}

func (c *DataUnit) AppendBinding(value interface{}) string {
	return c.Add(0, value)
}

func (c *DataUnit) UUID() string {
	newUUID := uuid.New()
	c.ParamsGroup = append(c.ParamsGroup, newUUID.String())
	return "?"
}

func (c *DataUnit) AsColumn(columnName string) (string, error) {
	return c.Columns.ColumnName(columnName)
}

func (c *DataUnit) Add(_ int, value interface{}) string {
	if value == nil {
		return ""
	}
	valueCopy, expanded := c.expandCopy(value)
	if valueCopy == nil {
		return ""
	}
	c.ParamsGroup = append(c.ParamsGroup, valueCopy...)
	return expanded
}

func (c *DataUnit) expandCopy(value interface{}) ([]interface{}, string) {
	switch actual := value.(type) {
	case *string:
		return []interface{}{actual}, "?"
	case *int:
		return []interface{}{actual}, "?"
	case *int64:
		return []interface{}{actual}, "?"
	case *uint64:
		return []interface{}{actual}, "?"
	case *float32:
		return []interface{}{actual}, "?"
	case *float64:
		return []interface{}{actual}, "?"
	case *uint:
		return []interface{}{actual}, "?"
	case *bool:
		return []interface{}{actual}, "?"
	case *int8:
		return []interface{}{actual}, "?"
	case *uint8:
		return []interface{}{actual}, "?"
	case *int32:
		return []interface{}{actual}, "?"
	case *uint32:
		return []interface{}{actual}, "?"
	case *int16:
		return []interface{}{actual}, "?"
	case *uint16:
		return []interface{}{actual}, "?"
	}
	srcValue := reflect.ValueOf(value)
	valuePtr := xunsafe.AsPointer(value)
	if srcValue.Kind() == reflect.Slice {
		return c.copyAndExpandSlice(srcValue.Type(), valuePtr)
	}

	dstValue := reflect.New(srcValue.Type())
	dstValue.Elem().Set(srcValue)
	valueCopy := dstValue.Elem().Interface()

	return []interface{}{valueCopy}, "?"
}

func (c *DataUnit) copyAndExpandSlice(sliceType reflect.Type, valuePtr unsafe.Pointer) ([]interface{}, string) {
	c.ensureSliceIndex()
	xslice := c.xunsafeSlice(sliceType.Elem())
	sliceLen := xslice.Len(valuePtr)
	switch sliceLen {
	case 0:
		return nil, ""
	case 1:
		return []interface{}{xslice.ValueAt(valuePtr, 0)}, "?"
	default:
		builder := strings.Builder{}
		builder.WriteByte('?')
		placeholders := make([]interface{}, sliceLen)
		placeholders[0] = xslice.ValueAt(valuePtr, 0)

		for i := 1; i < sliceLen; i++ {
			builder.WriteString(", ?")
			placeholders[i] = xslice.ValueAt(valuePtr, i)
		}

		return placeholders, builder.String()
	}
}

func (c *DataUnit) At(_ int) []interface{} {
	return c.ParamsGroup
}

func (c *DataUnit) Next() (interface{}, error) {
	if c.Mock {
		return 0, nil
	}

	if c.placeholderCounter < len(c.ParamsGroup) {
		index := c.placeholderCounter
		c.placeholderCounter++
		return c.ParamsGroup[index], nil
	}

	return nil, fmt.Errorf("expected to got binding parameter, but noone was found")
}

func (c *DataUnit) ensureSliceIndex() {
	if c.sliceIndex != nil {
		return
	}

	c.sliceIndex = map[reflect.Type]*xunsafe.Slice{}
}

func (c *DataUnit) xunsafeSlice(valueType reflect.Type) *xunsafe.Slice {
	slice, ok := c.sliceIndex[valueType]
	if !ok {
		slice = xunsafe.NewSlice(reflect.SliceOf(valueType))
		c.sliceIndex[valueType] = slice
	}

	return slice
}

func (c *DataUnit) addAll(args ...interface{}) {
	c.ParamsGroup = append(c.ParamsGroup, args...)
}

func (c *DataUnit) IsServiceExec(SQL string) (*Executable, bool) {
	return c.Statements.LookupExecutable(SQL)
}

func (c *DataUnit) FilterExecutables(statements []string, stopOnNonExec bool) []*Executable {
	result := make([]*Executable, 0)

	for i := 0; i < len(statements); i++ {
		if len(c.Statements.Executable) <= i {
			break
		}

		executable, ok := c.Statements.LookupExecutable(statements[i])
		if !ok && stopOnNonExec {
			return result
		}

		result = append(result, executable)
	}

	return result
}

func (c *DataUnit) In(columnName string, args interface{}) (string, error) {
	of := reflect.ValueOf(args)
	switch of.Kind() {
	case reflect.Slice:
		if of.Len() == 0 {
			return "1=0", nil
		}
	}

	sb := &strings.Builder{}
	sb.WriteString(columnName)
	sb.WriteString(" IN (")
	sb.WriteString(c.AppendBinding(args))
	sb.WriteString(")")
	return sb.String(), nil
}

func (c *DataUnit) Delete(data interface{}, name string) (string, error) {
	return c.Statements.DeleteWithMarker(name, data), nil
}
