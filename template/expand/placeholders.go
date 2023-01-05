package expand

import (
	"context"
	"fmt"
	"github.com/google/uuid"
	"github.com/viant/datly/executor/sequencer"
	"github.com/viant/xunsafe"
	"reflect"
	"strings"
	"unsafe"
)

type (
	ColumnsSource interface {
		ColumnName(key string) (string, error)
	}

	SQLCriteria struct {
		Columns            ColumnsSource
		ParamsGroup        []interface{}
		Mock               bool
		PlaceholderCounter int
		TemplateSQL        string
		MetaSource         MetaSource

		sliceIndex           map[reflect.Type]*xunsafe.Slice
		executables          []*Executable
		lastTableExecutables map[string]*Executable
		markerIndex          int
		markers              []string
	}
)

func (c *SQLCriteria) Allocate(tableName string, dest interface{}, selector string) (string, error) {
	db, err := c.MetaSource.Db()
	if err != nil {
		fmt.Printf("error occured while connecting to DB %v\n", err.Error())

		return "", fmt.Errorf("error occurred while connecting to DB")
	}

	service := sequencer.New(context.Background(), db)
	return "", service.Next(tableName, dest, selector)
}

func (c *SQLCriteria) AsBinding(value interface{}) string {
	return c.Add(0, value)
}

func (c *SQLCriteria) AppendBinding(value interface{}) string {
	return c.Add(0, value)
}

func (c *SQLCriteria) UUID() string {
	newUUID := uuid.New()
	c.ParamsGroup = append(c.ParamsGroup, newUUID.String())
	return "?"
}

func (c *SQLCriteria) AsColumn(columnName string) (string, error) {
	return c.Columns.ColumnName(columnName)
}

func (c *SQLCriteria) Add(_ int, value interface{}) string {
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

func (c *SQLCriteria) expandCopy(value interface{}) ([]interface{}, string) {
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
	valueType := reflect.TypeOf(value)
	valuePtr := xunsafe.AsPointer(value)
	if valueType.Kind() == reflect.Slice {
		return c.copyAndExpandSlice(valueType, valuePtr)
	}

	valueCopy := reflect.New(valueType).Elem().Interface()
	if valuePtr != nil {
		xunsafe.Copy(xunsafe.AsPointer(valueCopy), valuePtr, int(valueType.Size()))
	}

	return []interface{}{valueCopy}, "?"
}

func (c *SQLCriteria) copyAndExpandSlice(sliceType reflect.Type, valuePtr unsafe.Pointer) ([]interface{}, string) {
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

func (c *SQLCriteria) At(_ int) []interface{} {
	return c.ParamsGroup
}

func (c *SQLCriteria) Next() (interface{}, error) {
	if c.Mock {
		return 0, nil
	}

	if c.PlaceholderCounter < len(c.ParamsGroup) {
		index := c.PlaceholderCounter
		c.PlaceholderCounter++
		return c.ParamsGroup[index], nil
	}

	return nil, fmt.Errorf("expected to got binding parameter, but noone was found")
}

func (c *SQLCriteria) ensureSliceIndex() {
	if c.sliceIndex != nil {
		return
	}

	c.sliceIndex = map[reflect.Type]*xunsafe.Slice{}
}

func (c *SQLCriteria) xunsafeSlice(valueType reflect.Type) *xunsafe.Slice {
	slice, ok := c.sliceIndex[valueType]
	if !ok {
		slice = xunsafe.NewSlice(reflect.SliceOf(valueType))
		c.sliceIndex[valueType] = slice
	}

	return slice
}

func (c *SQLCriteria) addAll(args ...interface{}) {
	c.ParamsGroup = append(c.ParamsGroup, args...)
}

func (c *SQLCriteria) IsServiceExec(SQL string) (*Executable, bool) {
	if len(c.executables) <= c.markerIndex {
		return nil, false
	}

	if strings.TrimSpace(SQL) == c.markers[c.markerIndex] {
		executable := c.executables[c.markerIndex]
		c.markerIndex++
		return executable, true
	}

	return nil, false
}
