package compiler

import (
	"fmt"
	"reflect"
	"strings"

	"github.com/viant/bindly"
	"github.com/viant/datly/spec"
)

// OutputBindingCompiler compiles selected runtime metadata slots through the same
// canonical field/tag resolver as input contracts. It creates no invocation state.
type OutputBindingCompiler struct {
	Component *spec.Component
	Type      reflect.Type
	Kinds     []string
}

func (c OutputBindingCompiler) CompileBindings() ([]bindly.BindingSpec, error) {
	if c.Component == nil {
		return nil, fmt.Errorf("output metadata component is required")
	}
	fields, err := newContractFields(c.Type)
	if err != nil {
		return nil, err
	}
	selected := map[string]bool{}
	for _, kind := range c.Kinds {
		selected[kind] = true
	}
	canonical := map[string]*spec.Parameter{}
	for _, param := range spec.EffectiveParameters(c.Component.Parameters) {
		if param == nil || !selected[strings.ToLower(param.Source.Kind)] {
			continue
		}
		field, found, err := fields.resolve(param)
		if err != nil {
			return nil, err
		}
		if !found {
			return nil, fmt.Errorf("output metadata field not found: %s", param.Name)
		}
		if previous := canonical[field.field.Name]; previous != nil {
			// Explicit output authority wins over an input/tag projection of the
			// same field. Input and output parameters have separate identities.
			if previous.EmitOutput != param.EmitOutput {
				if param.EmitOutput {
					canonical[field.field.Name] = param
				}
				continue
			}
			return nil, fmt.Errorf("ambiguous output metadata field: %s", param.Name)
		}
		canonical[field.field.Name] = param
	}
	bindings := make([]bindly.BindingSpec, 0)
	for _, field := range fields.items {
		binding, found := field.binding, field.tagged
		if param := canonical[field.field.Name]; param != nil {
			if param.Codec != nil {
				return nil, fmt.Errorf("output metadata codecs require explicit output compilation: %s", param.Name)
			}
			binding, found, err = bindingSpecFromParam(field.field, param, binding, found, ParamCodec{})
			if err != nil {
				return nil, err
			}
		}
		if found && selected[binding.Location.Kind] {
			bindings = append(bindings, binding)
		}
	}
	return bindings, nil
}
