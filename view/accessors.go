package view

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
	ptr = a.upstream(ptr)
	a.xFields[len(a.xFields)-1].SetValue(ptr, value)
}

func (a *Accessor) setValue(ctx context.Context, ptr unsafe.Pointer, rawValue string, valueVisitor *Codec) error {
	ptr = a.upstream(ptr)
	xField := a.xFields[len(a.xFields)-1]

	if valueVisitor != nil {
		transformed, err := valueVisitor._visitorFn(ctx, rawValue)
		if err != nil {
			return err
		}
		xField.SetValue(ptr, transformed)
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

func (a *Accessor) upstream(ptr unsafe.Pointer) unsafe.Pointer {
	if len(a.xFields) == 1 {
		return ptr
	}
	for i := 0; i < len(a.xFields)-1; i++ {
		field := a.xFields[i]
		p := field.Pointer(ptr)
		if field.Kind() == reflect.Ptr && field.ValuePointer(ptr) == nil {
			newValue := reflect.New(field.Type.Elem()).Interface()
			field.SetValue(ptr, newValue)
		}
		p = field.Pointer(ptr)
		if field.Kind() == reflect.Ptr {
			p = xunsafe.DerefPointer(p)
		}
		ptr = p
	}
	return ptr
}

func (a *Accessor) Value(values interface{}) (interface{}, error) {
	ptr := xunsafe.AsPointer(values)
	pointer := a.upstream(ptr)
	xField := a.xFields[len(a.xFields)-1]
	v := xField.Value(pointer)
	return v, nil
}

func (a *Accessor) setBool(ptr unsafe.Pointer, value bool) {
	ptr = a.upstream(ptr)
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
