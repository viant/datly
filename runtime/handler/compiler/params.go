package compiler

import (
	"fmt"
	"reflect"
	"strings"

	"github.com/viant/bindly"
	"github.com/viant/datly/spec"
	"github.com/viant/datly/tag"
)

type contractField struct {
	field   reflect.StructField
	binding bindly.BindingSpec
	tagged  bool
}

type contractFields struct {
	typeOf reflect.Type
	items  []contractField
	index  *tag.BindingIndex
}

func newContractFields(inputType reflect.Type) (*contractFields, error) {
	bindingIndex, err := tag.NewBindingIndex(inputType)
	if err != nil {
		return nil, err
	}
	index := &contractFields{typeOf: inputType, index: bindingIndex}
	if inputType == nil || inputType.Kind() != reflect.Struct {
		return index, nil
	}
	for _, field := range bindingIndex.Fields() {
		index.items = append(index.items, contractField{field: field.Field, binding: field.Binding, tagged: field.Tagged})
	}
	return index, nil
}

func (c *contractFields) fieldParams(component *spec.Component) (map[string]*spec.Parameter, error) {
	if component == nil || c == nil || c.typeOf == nil || c.typeOf.Kind() != reflect.Struct {
		return nil, nil
	}
	result := map[string]*spec.Parameter{}
	for _, param := range spec.EffectiveParameters(component.Parameters) {
		if !isInputParam(param) {
			continue
		}
		field, ok, err := c.resolve(param)
		if err != nil {
			return nil, err
		}
		if !ok {
			continue
		}
		if existing := result[field.field.Name]; existing != nil && existing != param {
			return nil, fmt.Errorf("input field %s matches both parameters %q and %q", field.field.Name, existing.Name, param.Name)
		}
		result[field.field.Name] = param
	}
	if len(result) == 0 {
		return nil, nil
	}
	return result, nil
}

func (c *contractFields) resolve(param *spec.Parameter) (contractField, bool, error) {
	if c == nil || param == nil {
		return contractField{}, false, nil
	}
	field, ok, err := c.index.Resolve(param)
	if err != nil || !ok {
		return contractField{}, false, err
	}
	for _, item := range c.items {
		if item.field.Name == field.Name {
			return item, true, nil
		}
	}
	return contractField{}, false, nil
}

func isInputParam(param *spec.Parameter) bool {
	return param != nil && !param.EmitOutput && !strings.EqualFold(strings.TrimSpace(param.Source.Kind), "output")
}
