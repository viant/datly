package types

import (
	"fmt"
	"reflect"
)

var StrErrType = reflect.TypeOf(fmt.Errorf(""))

func IsObject(anError interface{}) bool {
	rType := reflect.TypeOf(anError)
	if rType == StrErrType {
		return false
	}

	for rType.Kind() == reflect.Ptr || rType.Kind() == reflect.Slice {
		rType = rType.Elem()
	}

	return rType.Kind() == reflect.Struct
}

func IsMulti(destType reflect.Type) bool {
	return destType.Kind() == reflect.Slice || destType.Kind() == reflect.Array
}
