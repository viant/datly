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
func NewCollector(relations []*data.Relation, resolver *Resolver, slicePtr interface{}) *Collector {
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

func createFieldCollectors(relations []*data.Relation, resolver *Resolver, parent interface{}) map[string]FieldCollector {
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
		fieldName := relations[relationIndex].Ref.On.RefHolder
		matcher, err := NewMatcher(relations[relationIndex].Ref.On, parentStructType, relations[relationIndex].Child.DataType())

		if err != nil {
			panic(err)
		}

		switch relations[relationIndex].Ref.Cardinality {
		case "One":
			columnResolver := resolver.ColumnResolver(relations[relationIndex].Ref.On.Column)
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
			//fmt.Printf("slice type :%v \n", relations[relationIndex].Child.DataType())
			refSlice := xunsafe.NewSlice(relations[relationIndex].Child.DataType())
			//fmt.Printf("%+v\n", parent)
			for i := 0; i < parentSliceLen; i++ {
				//Value -> pointer Employee#Id
				parentItemPtr := parentSlice.PointerAt(parentSlicePtr, uintptr(i))
				if parentCompType.Kind() == reflect.Ptr {
					parentItemPtr = xunsafe.DerefPointer(parentItemPtr)
				}
				key := matcher.ColumnValue(parentItemPtr)
				_parent[key] = parentItemPtr
			}

			result[fieldName] = func(refValue interface{}) {
				//fmt.Printf("RefValue: %v , %T, %p, %p\n", refValue, refValue, refValue, parent)
				id := matcher.ColumnRefValue(xunsafe.AsPointer(refValue))
				if appender, ok := appenders[id]; ok {
					appender.Append(refValue)
					return
				}
				parentRef := _parent[id]
				//fmt.Printf("ParentValue: %v\n", parentRef)
				//fmt.Printf("ParentRefId: %v\n", parentXColumn.Value(parentRef))
				//fmt.Printf("RefSlice type: %v\n", refSlice.Type)
				aSlice := reflect.MakeSlice(refSlice.Type, 0, 1)
				//fmt.Printf("Accounts type: %v\n", reflect.New(refSlice.Type).Type().String())
				matcher.SetPlaceholderValue(parentRef, xunsafe.ValuePointer(&aSlice))
				//fmt.Printf("before %v %T\n", fieldXField.Addr(parentRef), fieldXField.Addr(parentRef))
				appender := refSlice.Appender(matcher.PlaceholderPointer(parentRef))
				appenders[id] = appender
				appender.Append(refValue)
				//fmt.Printf("afet %v %T\n", fieldXField.Addr(parentRef), fieldXField.Addr(parentRef))
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
