package reader

import (
	"reflect"
)

func combineSlices(slice1, slice2 interface{}) interface{} {
	sliceVal1 := reflect.ValueOf(slice1)
	if sliceVal1.Kind() == reflect.Ptr {
		sliceVal1 = sliceVal1.Elem()
	}

	sliceVal2 := reflect.ValueOf(slice2)
	if sliceVal2.Kind() == reflect.Ptr {
		sliceVal2 = sliceVal2.Elem()
	}
	// Create a new slice with the combined length of slice1 and slice2
	sum := sliceVal1.Len() + sliceVal2.Len()
	combined := reflect.MakeSlice(reflect.SliceOf(sliceVal1.Type().Elem()), sum, sum)
	// Copy elements from the first slice
	reflect.Copy(combined, sliceVal1)
	// Copy elements from the second slice
	reflect.Copy(combined.Slice(sliceVal1.Len(), combined.Len()), sliceVal2)
	return combined.Interface()
}
