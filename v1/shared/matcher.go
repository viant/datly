package shared

//
//import (
//	"fmt"
//	"github.com/viant/datly/v1/data"
//	"github.com/viant/toolbox/format"
//	"github.com/viant/xunsafe"
//	"reflect"
//	"unsafe"
//)
//
////columnField -> Employee#Id
////columnRefField -> Account#Employee_Id
////placeholderField -> Accounts
//type ColumnMatcher struct {
//	placeholderField *xunsafe.Field
//
//	columnRefField *xunsafe.Field
//	columnField    *xunsafe.Field
//}
//
//func NewMatcher(columnMatch *data.RelationOwner, parentType reflect.Type, childType reflect.Type, caseFormater format.Case) (*ColumnMatcher, error) {
//	if parentType.Kind() == reflect.Ptr {
//		parentType = parentType.Elem()
//	}
//
//	if parentType.Kind() != reflect.Struct {
//		return nil, fmt.Errorf("parentType has to be a struct or pointer but was %v", parentType.String())
//	}
//
//	if childType.Kind() == reflect.Ptr {
//		childType = childType.Elem()
//	}
//
//	if childType.Kind() != reflect.Struct {
//		return nil, fmt.Errorf("childType has to be a struct or pointer but was %v", parentType.String())
//	}
//
//	placeholderField := xunsafe.FieldByName(parentType, columnMatch.Holder)
//	columnField := xunsafe.FieldByName(parentType, caseFormater.Format(columnMatch.Column, format.CaseUpperCamel))
//	columnRefField := xunsafe.FieldByName(childType, caseFormater.Format(columnMatch.Of.Column, format.CaseUpperCamel))
//	return &ColumnMatcher{
//		placeholderField: placeholderField,
//		columnField:      columnField,
//		columnRefField:   columnRefField,
//	}, nil
//}
//
//func (c *ColumnMatcher) ColumnRefValue(ptr unsafe.Pointer) interface{} {
//	return c.columnRefField.Value(ptr)
//}
//
//func (c *ColumnMatcher) ColumnValue(ptr unsafe.Pointer) interface{} {
//	return c.columnField.Value(ptr)
//}
//
//func (c *ColumnMatcher) PlaceholderValue(ptr unsafe.Pointer) interface{} {
//	return c.placeholderField.Value(ptr)
//}
//
//func (c *ColumnMatcher) SetPlaceholderValue(ptr unsafe.Pointer, value interface{}) {
//	c.placeholderField.SetValue(ptr, value)
//}
//
//func (c *ColumnMatcher) PlaceholderPointer(ptr unsafe.Pointer) unsafe.Pointer {
//	return c.placeholderField.Pointer(ptr)
//}
