package types

import (
	"reflect"
)

func NewValue(p reflect.Type) interface{} {
	return newReflectValue(p).Interface()
}

func newReflectValue(p reflect.Type) reflect.Value {
	if p.Kind() == reflect.Ptr {
		return reflect.New(p.Elem())
	}
	return reflect.New(p).Elem()
}
