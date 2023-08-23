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
	Locators   *KindLocator
}

func (p *Group) Names() []string {
	return nil
}

func (p *Group) Value(ctx context.Context, name string) (interface{}, bool, error) {

	parameter, ok := p.Parameters[name]
	if !ok {
		return nil, false, fmt.Errorf("uknonw parameter: %s", name)
	}

	stateType := structology.NewStateType(parameter.Schema.Type())
	aState := stateType.NewState()

	for _, item := range parameter.Group {
		value, has, err := p.ParameterLookup(ctx, item, p.Locators)
		if err != nil {
			return nil, false, err
		}
		if !has {
			continue
		}
		if err = aState.SetValue(item.Name, value); err != nil {
			return nil, false, err
		}
	}
	return aState.State(), true, nil
}

// NewGroup returns parameter locator
func NewGroup(opts ...Option) (kind.Locator, error) {
	options := NewOptions(opts)
	if options.ParameterLookup == nil {
		return nil, fmt.Errorf("parameterLookup was empty")
	}
	if options.Parameters == nil {
		return nil, fmt.Errorf("parameters was empty")
	}
	return &Group{
		ParameterLookup: options.ParameterLookup,
		Parameters:      options.Parameters,
		Locators:        options.Parent,
	}, nil
}
