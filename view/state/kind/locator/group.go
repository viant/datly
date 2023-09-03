package locator

import (
	"context"
	"fmt"
	"github.com/viant/datly/view/state"
	"github.com/viant/datly/view/state/kind"
	"github.com/viant/structology"
)

type Group struct {
	ParameterLookup
	Parameters state.NamedParameters
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
	return aState.State(), isAnyItemSet, nil
}

func (p *Group) matchByLocation(names string) *state.Parameter {
	var parameter *state.Parameter
	for _, candidate := range p.Parameters {
		if candidate.In.Kind == state.KindGroup && candidate.In.Name == names {
			parameter = candidate
		}
	}
	return parameter
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
		ParameterLookup: options.ParameterLookup,
		Parameters:      options.InputParameters,
	}, nil
}
