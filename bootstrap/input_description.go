package bootstrap

import (
	"reflect"

	handlercompiler "github.com/viant/datly/runtime/handler/compiler"
	"github.com/viant/datly/runtime/registry"
	"github.com/viant/datly/spec"
)

// DescribeInputContract assembles canonical authoring metadata for a resolved
// component through the existing binding compiler. It does not construct codec
// providers; the returned description must not be used for execution.
func DescribeInputContract(component *spec.Component, inputType reflect.Type, lookup func(string) (reflect.Type, error)) (*registry.InputContract, error) {
	result, err := handlercompiler.New(handlercompiler.Input{Component: component, InputType: inputType, TypeLookup: lookup}).Describe()
	if err != nil {
		return nil, err
	}
	return result.Input, nil
}
