package reader

import (
	"fmt"
	"github.com/viant/datly/v1/data"
	"github.com/viant/xunsafe"
	"reflect"
	"strings"
	"unsafe"
)

//columnField -> Employee#Id
//columnRefField -> Account#Employee_Id
//placeholderField -> Accounts
type ColumnMatcher struct {
	placeholderField *xunsafe.Field

	columnRefField *xunsafe.Field
	columnField    *xunsafe.Field
}

func NewMatcher(columnMatch *data.ColumnMatch, parentType reflect.Type, childType reflect.Type) (*ColumnMatcher, error) {
	if parentType.Kind() == reflect.Ptr {
		parentType = parentType.Elem()
	}

	if parentType.Kind() != reflect.Struct {
		return nil, fmt.Errorf("parentType has to be a struct or pointer but was %v", parentType.String())
	}

	if childType.Kind() == reflect.Ptr {
		childType = childType.Elem()
	}

	if childType.Kind() != reflect.Struct {
		return nil, fmt.Errorf("childType has to be a struct or pointer but was %v", parentType.String())
	}

	return &ColumnMatcher{
		placeholderField: xunsafe.FieldByName(parentType, strings.Title(columnMatch.RefHolder)),
		columnField:      xunsafe.FieldByName(parentType, strings.Title(columnMatch.Column)),
		columnRefField:   xunsafe.FieldByName(childType, strings.Title(columnMatch.RefColumn)),
	}, nil
}

func (c *ColumnMatcher) ColumnRefValue(ptr unsafe.Pointer) interface{} {
	return c.columnRefField.Value(ptr)
}

func (c *ColumnMatcher) ColumnValue(ptr unsafe.Pointer) interface{} {
	return c.columnField.Value(ptr)
}

func (c *ColumnMatcher) PlaceholderValue(ptr unsafe.Pointer) interface{} {
	return c.placeholderField.Value(ptr)
}

func (c *ColumnMatcher) SetPlaceholderValue(ptr unsafe.Pointer, value interface{}) {
	c.placeholderField.SetValue(ptr, value)
}

func (c *ColumnMatcher) PlaceholderPointer(ptr unsafe.Pointer) unsafe.Pointer {
	return c.placeholderField.Pointer(ptr)
}
