package view

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/viant/xunsafe"
	"reflect"
	"strconv"
	"strings"
	"unsafe"
)

type (
	Accessors struct {
		index     map[string]int
		namer     Namer
		accessors []*Accessor
	}

	Accessor struct {
		xFields []*xunsafe.Field
		xSlices []*xunsafe.Slice
	}
)

func (a *Accessor) set(ptr unsafe.Pointer, value interface{}) {
	ptr, _ = a.upstream(ptr)
	a.xFields[len(a.xFields)-1].SetValue(ptr, value)
}

func (a *Accessor) setValue(ctx context.Context, ptr unsafe.Pointer, rawValue string, valueVisitor *Codec, selector *Selector) error {
	ptr, _ = a.upstream(ptr)
	xField := a.xFields[len(a.xFields)-1]

	if valueVisitor != nil {
		transformed, err := valueVisitor._codecFn(ctx, rawValue, selector)
		if err != nil {
			return err
		}

		if transformed != nil {
			xField.SetValue(ptr, transformed)
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

	case reflect.Struct:
		dest := reflect.New(xField.Type)
		if err := json.Unmarshal([]byte(rawValue), dest.Interface()); err != nil {
			return err
		}

		xField.SetValue(ptr, dest.Elem().Interface())
		return nil
	}

	return fmt.Errorf("unsupported parameter type %v", xField.Type.String())
}

func (a *Accessor) upstream(ptr unsafe.Pointer, indexes ...int) (unsafe.Pointer, int) {
	if len(a.xFields) == 1 {
		return ptr, 0
	}

	indexCounter := 0
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

		if a.xSlices != nil && a.xSlices[i] != nil {
			p = a.xSlices[i].PointerAt(p, uintptr(indexes[indexCounter]))
			indexCounter++
		}

		ptr = p
	}
	return ptr, indexCounter
}

func (a *Accessor) Value(values interface{}, indexes ...int) (interface{}, error) {
	if values == nil {

	}
	ptr := xunsafe.AsPointer(values)
	pointer, index := a.upstream(ptr, indexes...)
	xField := a.xFields[len(a.xFields)-1]
	v := xField.Value(pointer)

	if a.xSlices[len(a.xSlices)-1] != nil && len(indexes) > index {
		v = a.xSlices[len(a.xSlices)-1].ValueAt(xField.Pointer(pointer), indexes[index])
	}

	return v, nil
}

func (a *Accessor) setBool(ptr unsafe.Pointer, value bool) {
	ptr, _ = a.upstream(ptr)
	a.xFields[len(a.xFields)-1].SetBool(ptr, value)
}

func (a *Accessors) indexAccessors(prefix string, parentType reflect.Type, fields []*xunsafe.Field) {
	parentType = elem(parentType)
	if parentType.Kind() != reflect.Struct {
		return
	}

	for i := 0; i < parentType.NumField(); i++ {
		field := parentType.Field(i)
		names := a.namer.Names(field)

		accessorFields := make([]*xunsafe.Field, len(fields)+1)
		copy(accessorFields, fields)
		accessorFields[len(accessorFields)-1] = xunsafe.NewField(field)

		for _, name := range names {
			accessorName := prefix + name
			a.indexAccessor(accessorName, accessorFields)
			a.indexAccessors(accessorName+".", field.Type, accessorFields)
		}
	}
}

func (a *Accessors) indexAccessor(name string, fields []*xunsafe.Field) {
	fieldAccessor := &Accessor{
		xFields: fields,
	}

	fieldAccessor.xSlices = make([]*xunsafe.Slice, len(fields))

	for i, field := range fields {
		if field.Kind() == reflect.Slice {
			fieldAccessor.xSlices[i] = xunsafe.NewSlice(field.Type)
		}
	}

	a.index[name] = len(a.accessors)
	a.accessors = append(a.accessors, fieldAccessor)
}

func (a *Accessors) init(rType reflect.Type) {
	if a.namer == nil {
		a.namer = &VeltyNamer{}
	}

	a.indexAccessors("", rType, []*xunsafe.Field{})
}

func (a *Accessors) AccessorByName(name string) (*Accessor, error) {
	i, ok := a.index[name]
	if !ok {
		return nil, fmt.Errorf("not found accessor for param %v", name)
	}

	return a.accessors[i], nil
}
