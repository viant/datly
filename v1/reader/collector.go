package reader

import (
	"fmt"
	"github.com/viant/datly/v1/data"
	"github.com/viant/xunsafe"
	"reflect"
	"unsafe"
)

type FieldCollector func(value interface{})

type Collector struct {
	data            interface{}
	resolver        *Resolver
	fieldCollectors map[string]FieldCollector
}

//NewCollector
//slice has to be a pointer to a slice of pointers
func NewCollector(relations []*data.Reference, resolver *Resolver, slicePtr interface{}) *Collector {
	rType := reflect.TypeOf(slicePtr)
	if rType.Kind() != reflect.Ptr {
		panic("slicePtr has to be a pointer to a slicePtr of pointers")
	}

	rType = rType.Elem()
	if rType.Kind() != reflect.Slice {
		panic("slicePtr has to be a pointer to a slicePtr of pointers")
	}

	rType = rType.Elem()
	if rType.Kind() != reflect.Ptr {
		panic("slicePtr has to be a pointer to a slicePtr of pointers")
	}

	return &Collector{
		data:            slicePtr,
		resolver:        resolver,
		fieldCollectors: createFieldCollectors(relations, resolver, slicePtr),
	}
}

func createFieldCollectors(relations []*data.Reference, resolver *Resolver, parent interface{}) map[string]FieldCollector {
	//fmt.Printf("%T\n", parent)
	parentStructType, err := structType(reflect.TypeOf(parent))
	if err != nil {
		panic(err)
	}
	parentCompType, err := sliceComponentType(reflect.TypeOf(parent))
	if err != nil {
		panic(err)
	}
	parentSlice := xunsafe.NewSlice(reflect.PtrTo(parentStructType))
	parentSlicePtr := xunsafe.AsPointer(parent)
	parentSliceLen := parentSlice.Len(parentSlicePtr)

	result := make(map[string]FieldCollector)
	for relationIndex := range relations {
		fieldName := relations[relationIndex].RefHolder
		matcher, err := NewMatcher(relations[relationIndex], parentStructType, relations[relationIndex].Child.DataType())

		if err != nil {
			panic(err)
		}

		switch relations[relationIndex].Cardinality {
		case "One":
			columnResolver := resolver.ColumnResolver(relations[relationIndex].Column)
			result[fieldName] = func(value interface{}) {
				fieldValue := matcher.ColumnRefValue(xunsafe.AsPointer(value))
				index, found := columnResolver.Value(fieldValue)
				if !found {
					return
				}
				sliceValuePtr := parentSlice.ValueAt(parentSlicePtr, index)
				matcher.SetPlaceholderValue(xunsafe.AsPointer(sliceValuePtr), value)
			}

		case "Many":
			appenders := make(map[interface{}]*xunsafe.Appender)
			_parent := make(map[interface{}]unsafe.Pointer)
			refSlice := xunsafe.NewSlice(relations[relationIndex].Child.DataType())
			for i := 0; i < parentSliceLen; i++ {
				parentItemPtr := parentSlice.PointerAt(parentSlicePtr, uintptr(i))
				if parentCompType.Kind() == reflect.Ptr {
					parentItemPtr = xunsafe.DerefPointer(parentItemPtr)
				}
				key := matcher.ColumnValue(parentItemPtr)
				_parent[key] = parentItemPtr
			}

			result[fieldName] = func(refValue interface{}) {
				id := matcher.ColumnRefValue(xunsafe.AsPointer(refValue))
				if appender, ok := appenders[id]; ok {
					appender.Append(refValue)
					return
				}
				parentRef := _parent[id]
				aSlice := reflect.MakeSlice(refSlice.Type, 0, 1)
				matcher.SetPlaceholderValue(parentRef, xunsafe.ValuePointer(&aSlice))
				appender := refSlice.Appender(matcher.PlaceholderPointer(parentRef))
				appenders[id] = appender
				appender.Append(refValue)
			}
		}
	}
	return result
}

func structType(rType reflect.Type) (reflect.Type, error) {
	switch rType.Kind() {
	case reflect.Ptr:
		return structType(rType.Elem())
	case reflect.Slice:
		return structType(rType.Elem())
	case reflect.Struct:
		return rType, nil
	}
	return nil, fmt.Errorf("invalid type %v", rType.String())
}

func sliceComponentType(rType reflect.Type) (reflect.Type, error) {
	switch rType.Kind() {
	case reflect.Ptr:
		return sliceComponentType(rType.Elem())
	case reflect.Slice:
		return rType.Elem(), nil
	}
	return nil, fmt.Errorf("invalid type %v", rType.String())
}

func (c *Collector) Collect(value interface{}, field string) {
	collect := c.fieldCollectors[field]
	collect(value)
}
