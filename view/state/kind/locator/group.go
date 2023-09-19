package locator

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/viant/datly/view/state"
	"github.com/viant/datly/view/state/kind"
	"github.com/viant/structology"
)

type Group struct {
	ParameterLookup

	InputParameters  state.NamedParameters
	OutputParameters state.NamedParameters
}

func (p *Group) Names() []string {
	return nil
}

func (p *Group) Value(ctx context.Context, names string) (interface{}, bool, error) {
	parameter := p.matchByLocation(names)
	if parameter == nil {
		return nil, false, fmt.Errorf("failed to match parameter by location: %v", names)
	}
	stateType := structology.NewStateType(parameter.Schema.Type())
	aState := stateType.NewState()

	isAnyItemSet := false
	for _, item := range parameter.Group {
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
	dd, _ := json.Marshal(ret)
	fmt.Printf("%T %s\n", ret, dd)

	return ret, isAnyItemSet, nil
}

//func (p *Group) matchByLocation(names string) *state.Parameter {
//	var parameter *state.Parameter
//	for _, candidate := range p.Parameters {
//		if candidate.In.Kind == state.KindGroup && candidate.In.Name == names {
//			parameter = candidate
//		}
//	}
//	return parameter
//}

func (p *Group) matchByLocation(names string) *state.Parameter {
	matched := p.OutputParameters.LookupByLocation(state.KindGroup, names)
	if matched == nil {
		matched = p.InputParameters.LookupByLocation(state.KindGroup, names)
	}
	return matched
}

// NewGroup returns parameter locator
func NewGroup(opts ...Option) (kind.Locator, error) {
	options := NewOptions(opts)
	if options.ParameterLookup == nil {
		return nil, fmt.Errorf("parameterLookup was empty")
	}
	if options.InputParameters == nil {
		return nil, fmt.Errorf("parameters was empty")
	}
	return &Group{
		ParameterLookup:  options.ParameterLookup,
		InputParameters:  options.InputParameters,
		OutputParameters: options.OutputParameters,
	}, nil
}
