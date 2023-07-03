package types

import (
	"github.com/viant/sqlx/io"
	"github.com/viant/xreflect"
	"reflect"
)

func LookupType(lookup xreflect.LookupType, typeName string, opts ...xreflect.Option) (reflect.Type, error) {
	rType, ok := io.ParseType(typeName)
	if ok {
		return rType, nil
	}
	return lookup(typeName, opts...)
}

func Elem(rType reflect.Type) reflect.Type {
	for rType.Kind() == reflect.Ptr || rType.Kind() == reflect.Slice {
		rType = rType.Elem()
	}

	return rType
}

func EnsureStruct(fType reflect.Type) reflect.Type {
	switch fType.Kind() {
	case reflect.Ptr:
		return EnsureStruct(fType.Elem())
	case reflect.Slice:
		return EnsureStruct(fType.Elem())
	case reflect.Struct:
		return fType
	}
	return nil
}
