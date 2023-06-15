package types

import (
	"reflect"
)

func NewValue(p reflect.Type) interface{} {
	return NewRValue(p).Interface()
}

func NewRValue(p reflect.Type) reflect.Value {
	if p.Kind() == reflect.Ptr {
		return reflect.New(p.Elem())
	}

	return reflect.New(p).Elem()
}
