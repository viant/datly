package locator

import (
	"context"
	"fmt"
	"github.com/viant/datly/view/state"
	"github.com/viant/datly/view/state/kind"
	"github.com/viant/structology"
	"reflect"
)

type Object struct {
	ParameterLookup

	InputParameters  state.NamedParameters
	OutputParameters state.NamedParameters
}

func (p *Object) Names() []string {
	return nil
}

func (p *Object) Value(ctx context.Context, _ reflect.Type, names string) (interface{}, bool, error) {
	parameter := p.matchByLocation(names)
	if parameter == nil {
		return nil, false, fmt.Errorf("failed to match parameter by location: %v", names)
	}
	stateType := structology.NewStateType(parameter.Schema.Type())
	aState := stateType.NewState()

	isAnyItemSet := false
	for _, item := range parameter.Object {
		value, has, err := p.ParameterLookup(ctx, item)
		if err != nil {
			return nil, false, err
		}
		if !has {
			continue
		}
		isAnyItemSet = true
		if err = aState.SetValue(item.Name, value); err != nil {
			return nil, false, err
		}
	}
	ret := aState.State()
	return ret, isAnyItemSet, nil
}

//func (p *NormalizeObject) matchByLocation(names string) *state.Parameter {
//	var parameter *state.Parameter
//	for _, candidate := range p.Parameters {
//		if candidate.In.Kind == state.KindObject && candidate.In.Name == names {
//			parameter = candidate
//		}
//	}
//	return parameter
//}

func (p *Object) matchByLocation(names string) *state.Parameter {
	matched := p.OutputParameters.LookupByLocation(state.KindObject, names)
	if matched == nil {
		matched = p.InputParameters.LookupByLocation(state.KindObject, names)
	}
	return matched
}

// NewObject returns parameter locator
func NewObject(opts ...Option) (kind.Locator, error) {
	options := NewOptions(opts)
	if options.ParameterLookup == nil {
		return nil, fmt.Errorf("parameterLookup was empty")
	}
	if options.InputParameters == nil {
		return nil, fmt.Errorf("parameters was empty")
	}
	return &Object{
		ParameterLookup:  options.ParameterLookup,
		InputParameters:  options.InputParameters,
		OutputParameters: options.OutputParameters,
	}, nil
}
