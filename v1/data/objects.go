package data

import (
	"github.com/viant/xunsafe"
	"reflect"
	"unsafe"
)

func Index(relation *Relation, slicePtr unsafe.Pointer, slice *xunsafe.Slice, componentType reflect.Type) map[interface{}]unsafe.Pointer {
	shallDeref := componentType.Kind() == reflect.Ptr
	field := relation.columnField
	result := make(map[interface{}]unsafe.Pointer)
	for i := 0; i < slice.Len(slicePtr); i++ {
		ptr := slice.PointerAt(slicePtr, uintptr(i))
		if shallDeref {
			ptr = xunsafe.DerefPointer(ptr)
		}
		result[field.Value(ptr)] = ptr
	}

	return result
}
