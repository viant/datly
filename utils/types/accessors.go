package types

import (
	"encoding/json"
	"fmt"
	"github.com/viant/datly/converter"
	"github.com/viant/xunsafe"
	"reflect"
	"strconv"
	"strings"
	"time"
	"unsafe"
)

type (
	Accessors struct {
		index       map[string]int
		namer       Namer
		accessors   []*Accessor
		initialized bool
	}

	Accessor struct {
		isPtr   bool
		xType   *xunsafe.Type
		xFields []*xunsafe.Field
		xSlices []*xunsafe.Slice
	}
)

func NewAccessors(namer Namer) *Accessors {
	return &Accessors{
		namer: namer,
		index: map[string]int{},
	}
}

func NewAccessor(fields []*xunsafe.Field) *Accessor {
	var xType *xunsafe.Type
	var isPtr bool

	return &Accessor{
		isPtr:   isPtr,
		xType:   xType,
		xFields: fields,
	}
}

func (a *Accessor) Type() reflect.Type {
	return a.xFields[len(a.xFields)-1].Type
}

func (a *Accessor) SetValue(ptr unsafe.Pointer, value interface{}) {
	if value == nil {
		return
	}

	xField, ptr := a.actualPtr(ptr)
	xField.SetValue(ptr, value)
}

func (a *Accessor) SetConvertedAndGet(ptr unsafe.Pointer, value interface{}, format string) (interface{}, error) {
	return a.adjustAndSet(ptr, value, format)
}

func (a *Accessor) SetConverted(ptr unsafe.Pointer, value interface{}, format string) error {
	_, err := a.adjustAndSet(ptr, value, format)
	return err
}

func (a *Accessor) adjustAndSet(ptr unsafe.Pointer, value interface{}, format string) (interface{}, error) {
	if value == nil {
		return nil, nil
	}

	xField, ptr := a.actualPtr(ptr)
	switch xField.Type.Kind() {
	case reflect.String:
		switch actual := value.(type) {
		case *time.Time:
			result := actual.Format(time.RFC3339)
			xField.SetString(ptr, result)
			return result, nil
		case time.Time:
			result := actual.Format(time.RFC3339)
			xField.SetString(ptr, result)
			return result, nil
		case string:
			xField.SetString(ptr, actual)
			return actual, nil
		case int:
			result := strconv.Itoa(actual)
			xField.SetString(ptr, result)
			return result, nil
		case float64:
			result := strconv.FormatFloat(actual, 'f', -1, 64)
			xField.SetString(ptr, result)
			return result, nil
		case bool:
			result := strconv.FormatBool(actual)
			xField.SetString(ptr, result)
			return result, nil
		case int64:
			result := strconv.Itoa(int(actual))
			xField.SetString(ptr, result)
			return result, nil
		}

	case reflect.Int:
		switch actual := value.(type) {
		case string:
			atoi, err := strconv.Atoi(actual)
			if err != nil {
				return nil, err
			}
			xField.SetInt(ptr, atoi)
			return atoi, nil
		case int:
			xField.SetInt(ptr, actual)
			return actual, nil
		case int8:
			result := int(actual)
			xField.SetInt(ptr, result)
			return result, nil
		case int16:
			result := int(actual)
			xField.SetInt(ptr, result)
			return result, nil
		case int32:
			result := int(actual)
			xField.SetInt(ptr, result)
			return result, nil
		case int64:
			result := int(actual)
			xField.SetInt(ptr, result)
			return result, nil
		case uint:
			result := int(actual)
			xField.SetInt(ptr, result)
			return result, nil
		case uint8:
			result := int(actual)
			xField.SetInt(ptr, result)
			return result, nil
		case uint16:
			result := int(actual)
			xField.SetInt(ptr, result)
			return result, nil
		case uint32:
			result := int(actual)
			xField.SetInt(ptr, result)
			return result, nil
		case uint64:
			result := int(actual)
			xField.SetInt(ptr, result)
			return result, nil
		case float64:
			result := int(actual)
			xField.SetInt(ptr, result)
			return result, nil
		case float32:
			result := int(actual)
			xField.SetInt(ptr, result)
			return result, nil
		}

	case reflect.Bool:
		switch actual := value.(type) {
		case bool:
			xField.SetBool(ptr, actual)
			return actual, nil
		case string:
			parseBool, err := strconv.ParseBool(actual)
			if err != nil {
				return nil, err
			}

			result := parseBool
			xField.SetBool(ptr, result)
			return result, nil
		}

	case reflect.Float64:
		switch actual := value.(type) {
		case float64:
			xField.SetFloat64(ptr, actual)
			return actual, nil
		case float32:
			result := float64(actual)
			xField.SetFloat64(ptr, result)
			return result, nil
		case string:
			float, err := strconv.ParseFloat(actual, 64)
			if err != nil {
				return nil, err
			}

			xField.SetFloat64(ptr, float)
			return float, nil
		case int:
			result := float64(actual)
			xField.SetFloat64(ptr, result)
			return result, nil
		case int8:
			result := float64(actual)
			xField.SetFloat64(ptr, result)
			return result, nil
		case int16:
			result := float64(actual)
			xField.SetFloat64(ptr, result)
			return result, nil
		case int32:
			result := float64(actual)
			xField.SetFloat64(ptr, result)
			return result, nil
		case int64:
			result := float64(actual)
			xField.SetFloat64(ptr, result)
			return result, nil
		case uint:
			result := float64(actual)
			xField.SetFloat64(ptr, result)
			return result, nil
		case uint8:
			result := float64(actual)
			xField.SetFloat64(ptr, result)
			return result, nil
		case uint16:
			result := float64(actual)
			xField.SetFloat64(ptr, result)
			return result, nil
		case uint32:
			result := float64(actual)
			xField.SetFloat64(ptr, result)
			return result, nil
		case uint64:
			result := float64(actual)
			xField.SetFloat64(ptr, result)
			return result, nil
		}
	}

	if reflect.TypeOf(value) == xField.Type {
		xField.SetValue(ptr, value)
		return value, nil
	}

	marshal, err := json.Marshal(value)
	if err != nil {
		return nil, err
	}

	converted, _, err := converter.Convert(string(marshal), xField.Type, false, format)
	if err != nil {
		return nil, err
	}

	xField.SetValue(ptr, converted)
	return converted, nil
}

func (a *Accessor) actualPtr(ptr unsafe.Pointer) (*xunsafe.Field, unsafe.Pointer) {
	ptr, _ = a.upstream(ptr)
	xField := a.xFields[len(a.xFields)-1]

	return xField, ptr
}

func (a *Accessor) upstream(ptr unsafe.Pointer, indexes ...int) (unsafe.Pointer, int) {
	if a.isPtr {
		ptr = xunsafe.DerefPointer(ptr)
	}

	if ptr == nil {
		return nil, 0
	}

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
		return nil, nil
	}

	ptr := xunsafe.AsPointer(values)
	var index int
	ptr, index = a.upstream(ptr, indexes...)
	xField := a.xFields[len(a.xFields)-1]
	v := xField.Value(ptr)

	if a.xSlices[len(a.xSlices)-1] != nil && len(indexes) > index {
		v = a.xSlices[len(a.xSlices)-1].ValueAt(xField.Pointer(ptr), indexes[index])
	}

	return v, nil
}

func (a *Accessor) Values(values interface{}, indexes ...int) ([]interface{}, error) {
	if values == nil {
		return nil, nil
	}

	ptr := xunsafe.AsPointer(values)
	var index int
	ptr, index = a.upstream(ptr, indexes...)
	xField := a.xFields[len(a.xFields)-1]

	if xField.Type.Kind() != reflect.Slice {
		v := xField.Value(ptr)

		if (len(a.xSlices)) != 0 && a.xSlices[len(a.xSlices)-1] != nil && len(indexes) > index {
			v = a.xSlices[len(a.xSlices)-1].ValueAt(xField.Pointer(ptr), indexes[index])
		}

		return []interface{}{v}, nil
	}

	ptr = xField.Pointer(ptr)
	slice := a.xSlices[len(a.xSlices)-1]
	sliceLen := slice.Len(ptr)
	placeholders := make([]interface{}, sliceLen)

	for i := 0; i < sliceLen; i++ {
		placeholders[i] = slice.ValueAt(ptr, i)
	}

	return placeholders, nil
}

func (a *Accessor) SetBool(ptr unsafe.Pointer, value bool) {
	ptr, _ = a.upstream(ptr)
	a.xFields[len(a.xFields)-1].SetBool(ptr, value)
}

func (a *Accessor) String(ptr unsafe.Pointer) string {
	if ptr == nil {
		return ""
	}

	xField, ptr := a.actualPtr(ptr)
	return xField.String(ptr)
}

func (a *Accessors) indexAccessors(prefix string, parentType reflect.Type, fields []*xunsafe.Field, path string) {
	actualParentType := parentType

	parentType = Elem(parentType)
	if parentType.Kind() != reflect.Struct {
		return
	}

	numField := parentType.NumField()
	for i := 0; i < numField; i++ {
		field := parentType.Field(i)
		names := a.namer.Names(field)

		accessorFields := make([]*xunsafe.Field, len(fields)+1)
		copy(accessorFields, fields)
		accessorFields[len(accessorFields)-1] = xunsafe.NewField(field)

		for _, name := range names {
			accessorName := prefix + name
			if path != "" && !strings.HasPrefix(path, accessorName) {
				continue
			}

			a.indexAccessor(accessorName, accessorFields, actualParentType)
			a.indexAccessors(accessorName+".", field.Type, accessorFields, path)
		}
	}
}

func (a *Accessors) indexAccessor(name string, fields []*xunsafe.Field, parentType reflect.Type) {

	fieldAccessor := NewAccessor(fields)

	fieldAccessor.xSlices = make([]*xunsafe.Slice, len(fields))

	for i, field := range fields {
		if field.Kind() == reflect.Slice {
			fieldAccessor.xSlices[i] = xunsafe.NewSlice(field.Type)
		}
	}
	a.index[name] = len(a.accessors)
	a.accessors = append(a.accessors, fieldAccessor)
}

func (a *Accessors) Init(rType reflect.Type) {
	if a.init() {
		return
	}
	a.indexAccessors("", rType, []*xunsafe.Field{}, "")
}

func (a *Accessors) InitPath(rType reflect.Type, path string) {
	if a.init() {
		return
	}

	a.indexAccessors("", rType, []*xunsafe.Field{}, path)
}

func (a *Accessors) init() bool {
	if a.initialized {
		return true
	}

	a.initialized = true
	if a.namer == nil {
		a.namer = &VeltyNamer{}
	}
	return false
}

func (a *Accessors) AccessorByName(name string) (*Accessor, error) {
	i, ok := a.index[name]
	if !ok {
		return nil, fmt.Errorf("not found accessor for param %v", name)
	}

	return a.accessors[i], nil
}
