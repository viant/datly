package data

import (
	"context"
	"fmt"
	"github.com/viant/velty"
	"github.com/viant/xunsafe"
	"reflect"
	"strconv"
	"strings"
	"unsafe"
)

type (
	Accessors struct {
		index     map[string]int
		accessors []*Accessor
	}

	Accessor struct {
		xFields []*xunsafe.Field
	}
)

func (a *Accessor) set(ptr unsafe.Pointer, value interface{}) {
	ptr = a.actualStruct(ptr)
	a.xFields[len(a.xFields)-1].SetValue(ptr, value)
}

func (a *Accessor) actualStruct(ptr unsafe.Pointer) unsafe.Pointer {
	prev := ptr
	for i := 0; i < len(a.xFields)-1; i++ {
		ptr = a.xFields[i].ValuePointer(ptr)
		if ptr == nil {
			ptr = a.initValue(a.xFields[i], prev)
		}

		prev = ptr
	}

	if ptr == nil && len(a.xFields)-1 >= 0 {
		ptr = a.initValue(a.xFields[len(a.xFields)-1], prev)
	}

	return ptr
}

func (a *Accessor) initValue(field *xunsafe.Field, prev unsafe.Pointer) unsafe.Pointer {
	value := reflect.New(elem(field.Type))
	if field.Type.Kind() != reflect.Ptr {
		value = value.Elem()
	}

	field.SetValue(prev, value.Interface())
	return unsafe.Pointer(value.Pointer())
}

func (a *Accessor) setValue(ctx context.Context, ptr unsafe.Pointer, rawValue string, rawVisitor *RawVisitor, valueVisitor *ValueVisitor) error {
	xField := a.xFields[len(a.xFields)-1]

	var err error
	if rawVisitor != nil {
		rawValue, err = rawVisitor._visitorFn(rawValue)
		if err != nil {
			return err
		}
	}

	if valueVisitor != nil {
		transformed, err := valueVisitor._visitorFn(ctx, rawValue)
		if err != nil {
			return err
		}

		if err = valueVisitor._valueSetter(xField, ptr, transformed); err != nil {
			return err
		}
		return nil
	}

	//TODO: Add remaining types
	switch xField.Type.Kind() {
	case reflect.String:
		xField.SetValue(ptr, rawValue)
		return nil

	case reflect.Int:
		asInt, err := strconv.Atoi(rawValue)
		if err != nil {
			return err
		}
		xField.SetInt(ptr, asInt)
		return nil

	case reflect.Bool:
		xField.SetBool(ptr, strings.EqualFold(rawValue, "true"))
		return nil

	case reflect.Float64:
		asFloat, err := strconv.ParseFloat(rawValue, 64)
		if err != nil {
			return err
		}

		xField.SetFloat64(ptr, asFloat)
		return nil
	}

	return fmt.Errorf("unsupported parameter type %v", xField.Type.String())
}

func (a *Accessor) Value(values interface{}) (interface{}, error) {
	asPointer := xunsafe.AsPointer(values)
	pointer := a.actualStruct(asPointer)
	xField := a.xFields[len(a.xFields)-1]
	//TODO: Add remaining types
	switch xField.Type.Kind() {
	case reflect.Int:
		return xField.Int(pointer), nil
	case reflect.Float64:
		return xField.Float64(pointer), nil
	case reflect.Bool:
		return xField.Bool(pointer), nil
	case reflect.String:
		return xField.String(pointer), nil
	case reflect.Ptr, reflect.Struct:
		return xField.Value(pointer), nil
	default:
		return nil, fmt.Errorf("unsupported field type %v", xField.Type.String())
	}
}

func (a *Accessor) setBool(ptr unsafe.Pointer, value bool) {
	ptr = a.actualStruct(ptr)
	a.xFields[len(a.xFields)-1].SetBool(ptr, value)
}

func (a *Accessors) indexAccessors(prefix string, parentType reflect.Type, fields []*xunsafe.Field) {
	parentType = elem(parentType)
	if parentType.Kind() != reflect.Struct {
		return
	}

	for i := 0; i < parentType.NumField(); i++ {
		field := parentType.Field(i)
		fieldTag := velty.Parse(field.Tag.Get("velty"))

		accessorFields := make([]*xunsafe.Field, len(fields)+1)
		copy(accessorFields, fields)
		accessorFields[len(accessorFields)-1] = xunsafe.NewField(field)

		if len(fieldTag.Names) > 0 {
			for _, name := range fieldTag.Names {
				accessorName := prefix + name
				a.indexAccessor(accessorName, accessorFields)
				a.indexAccessors(accessorName+".", field.Type, accessorFields)
			}
		} else {
			accessorName := prefix + field.Name
			a.indexAccessor(accessorName, accessorFields)
			a.indexAccessors(accessorName+".", field.Type, accessorFields)
		}
	}
}

func (a *Accessors) indexAccessor(name string, fields []*xunsafe.Field) {
	fieldAccessor := &Accessor{
		xFields: fields,
	}

	a.index[name] = len(a.accessors)
	a.accessors = append(a.accessors, fieldAccessor)
}

func (a *Accessors) init(templateType reflect.Type) {
	a.indexAccessors("", templateType, []*xunsafe.Field{})
}
