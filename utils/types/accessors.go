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

func NewAccessor(fields []*xunsafe.Field, structType reflect.Type) *Accessor {
	var xType *xunsafe.Type
	var isPtr bool

	//if structType.Kind() == reflect.Ptr {
	//	xType = xunsafe.NewType(structType)
	//	isPtr = true
	//}

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

func (a *Accessor) AdjustAndSet(ptr unsafe.Pointer, value interface{}, format string) error {
	if value == nil {
		return nil
	}

	xField, ptr := a.actualPtr(ptr)
	switch xField.Type.Kind() {
	case reflect.String:
		switch actual := value.(type) {
		case *time.Time:
			xField.SetString(ptr, actual.Format(time.RFC3339))
			return nil
		case time.Time:
			xField.SetString(ptr, actual.Format(time.RFC3339))
			return nil
		case string:
			xField.SetString(ptr, actual)
			return nil
		case int:
			xField.SetString(ptr, strconv.Itoa(actual))
			return nil
		case float64:
			xField.SetString(ptr, strconv.FormatFloat(actual, 'f', -1, 64))
			return nil
		case bool:
			xField.SetString(ptr, strconv.FormatBool(actual))
			return nil
		case int64:
			xField.SetString(ptr, strconv.Itoa(int(actual)))
			return nil
		}

	case reflect.Int:
		switch actual := value.(type) {
		case string:
			atoi, err := strconv.Atoi(actual)
			if err != nil {
				return err
			}
			xField.SetInt(ptr, atoi)
			return nil
		case int:
			xField.SetInt(ptr, actual)
			return nil
		case int8:
			xField.SetInt(ptr, int(actual))
			return nil
		case int16:
			xField.SetInt(ptr, int(actual))
			return nil
		case int32:
			xField.SetInt(ptr, int(actual))
			return nil
		case int64:
			xField.SetInt(ptr, int(actual))
			return nil
		case uint:
			xField.SetInt(ptr, int(actual))
			return nil
		case uint8:
			xField.SetInt(ptr, int(actual))
			return nil
		case uint16:
			xField.SetInt(ptr, int(actual))
			return nil
		case uint32:
			xField.SetInt(ptr, int(actual))
			return nil
		case uint64:
			xField.SetInt(ptr, int(actual))
			return nil
		case float64:
			xField.SetInt(ptr, int(actual))
			return nil
		case float32:
			xField.SetInt(ptr, int(actual))
			return nil
		}

	case reflect.Bool:
		switch actual := value.(type) {
		case bool:
			xField.SetBool(ptr, actual)
			return nil
		case string:
			parseBool, err := strconv.ParseBool(actual)
			if err != nil {
				return err
			}
			xField.SetBool(ptr, parseBool)
			return nil
		}

	case reflect.Float64:
		switch actual := value.(type) {
		case float64:
			xField.SetFloat64(ptr, actual)
			return nil
		case float32:
			xField.SetFloat64(ptr, float64(actual))
			return nil
		case string:
			float, err := strconv.ParseFloat(actual, 64)
			if err != nil {
				return err
			}

			xField.SetFloat64(ptr, float)
			return nil
		case int:
			xField.SetFloat64(ptr, float64(actual))
			return nil
		case int8:
			xField.SetFloat64(ptr, float64(actual))
			return nil
		case int16:
			xField.SetFloat64(ptr, float64(actual))
			return nil
		case int32:
			xField.SetFloat64(ptr, float64(actual))
			return nil
		case int64:
			xField.SetFloat64(ptr, float64(actual))
			return nil
		case uint:
			xField.SetFloat64(ptr, float64(actual))
			return nil
		case uint8:
			xField.SetFloat64(ptr, float64(actual))
			return nil
		case uint16:
			xField.SetFloat64(ptr, float64(actual))
			return nil
		case uint32:
			xField.SetFloat64(ptr, float64(actual))
			return nil
		case uint64:
			xField.SetFloat64(ptr, float64(actual))
			return nil
		}
	}

	if reflect.TypeOf(value) == xField.Type {
		xField.SetValue(ptr, value)
		return nil
	}

	marshal, err := json.Marshal(value)
	if err != nil {
		return err
	}

	converted, _, err := converter.Convert(string(marshal), xField.Type, false, format)
	if err != nil {
		return err
	}

	xField.SetValue(ptr, converted)
	return nil
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
	fieldAccessor := NewAccessor(fields, parentType)

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
