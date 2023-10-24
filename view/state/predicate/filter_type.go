package predicate

import (
	"github.com/viant/datly/view/tags"
	"github.com/viant/xdatly/predicate"
	"reflect"
)

// FilterType represents fitler type
type FilterType struct {
	IncludeTag    string
	ExcludeTag    string
	ParameterType reflect.Type
	Tag           *tags.Predicate
}

func (f *FilterType) Type() reflect.Type {
	if isIntType(f.ParameterType) {
		return reflect.TypeOf(&predicate.IntFilter{})
	} else if isBoolType(f.ParameterType) {
		return reflect.TypeOf(&predicate.BoolFilter{})
	}
	return reflect.TypeOf(&predicate.StringsFilter{})
}

func (f *FilterType) StructTagTag() string {
	if f.IncludeTag != "" {
		return f.IncludeTag
	}
	return f.ExcludeTag
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
		return isBoolType(t.Elem())
	}
	return false
}
