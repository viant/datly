package shared

import (
	"reflect"
	"time"
)

var byteType = reflect.TypeOf([]byte{})
var timeType = reflect.TypeOf(time.Time{})


//IsBaseType return true if base type
func IsBaseType(aType reflect.Type) bool {
	if aType.Kind() == reflect.Ptr {
		aType = aType.Elem()
	}
	switch aType.Kind() {
	case reflect.Int, reflect.Int64, reflect.Int8, reflect.Int16, reflect.Int32,
		reflect.Uint, reflect.Uint64, reflect.Uint8, reflect.Uint16, reflect.Uint32,
		reflect.Float32, reflect.Float64,
		reflect.Bool, reflect.String:
		return true
	default:
		if byteType.AssignableTo(aType) || timeType.AssignableTo(aType) {
			return true
		}
	}
	return false
}

