package shared

import "reflect"

func Elem(rType reflect.Type) reflect.Type {
	switch rType.Kind() {
	case reflect.Ptr, reflect.Slice:
		return Elem(rType.Elem())
	}
	return rType
}
