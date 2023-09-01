package predicate

import (
	"github.com/viant/xdatly/predicate"
	"reflect"
)

// FilterType represents fitler type
type FilterType struct {
	*Tag
	IncludeTag    string
	ExcludeTag    string
	ParameterType reflect.Type
}

func (f *FilterType) Type() reflect.Type {
	if isIntType(f.ParameterType) {
		return reflect.TypeOf(&predicate.IntFilter{})
	} else if isBoolType(f.ParameterType) {
		return reflect.TypeOf(&predicate.BoolFilter{})
	}
	return reflect.TypeOf(&predicate.StringsFilter{})
}

func (f *FilterType) StructTagTag() reflect.StructTag {
	if f.IncludeTag != "" {
		return reflect.StructTag(f.IncludeTag)
	}
	return reflect.StructTag(f.ExcludeTag)
}

func isIntType(t reflect.Type) bool {
	switch t.Kind() {
	case reflect.Int:
		return true
	case reflect.Slice, reflect.Ptr:
		return isIntType(t.Elem())
	}
	return false
}

func isBoolType(t reflect.Type) bool {
	switch t.Kind() {
	case reflect.Bool:
		return true
	case reflect.Slice, reflect.Ptr:
		return isIntType(t.Elem())
	}
	return false
}
