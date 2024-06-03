package reader

import (
	"reflect"
)

func combineSlices(slice1, slice2 interface{}) interface{} {
	val1 := reflect.ValueOf(slice1)
	val2 := reflect.ValueOf(slice2)
	// Create a new slice with the combined length of slice1 and slice2
	combined := reflect.MakeSlice(reflect.SliceOf(val1.Type().Elem()), val1.Len()+val2.Len(), val1.Len()+val2.Len())
	// Copy elements from the first slice
	reflect.Copy(combined, val1)
	// Copy elements from the second slice
	reflect.Copy(combined.Slice(val1.Len(), combined.Len()), val2)
	return combined.Interface()
}
