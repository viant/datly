package expand

import (
	"reflect"
)

const lengthFunctionName = "Length"

type (
	stringLength struct{}
	arrayLength  struct{}
)

func (a *arrayLength) Kind() []reflect.Kind {
	return []reflect.Kind{reflect.Slice, reflect.Array}
}

func (a *arrayLength) Handler() interface{} {
	return func(aSlice interface{}) int {
		return reflect.ValueOf(aSlice).Len()
	}
}

func newStringLength() *stringLength {
	return &stringLength{}
}

func (s *stringLength) Kind() []reflect.Kind {
	return []reflect.Kind{reflect.String}
}

func (s *stringLength) Handler() interface{} {
	return func(aString string) int {
		return len(aString)
	}
}

func newArrayLength() *arrayLength {
	return &arrayLength{}
}
