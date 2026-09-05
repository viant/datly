package locator

import (
	"context"
	"fmt"
	"github.com/viant/datly/view/state"
	"github.com/viant/datly/view/state/kind"
	"reflect"
)

type Parameter struct {
	ParameterLookup
	Parameters state.NamedParameters
}

func (p *Parameter) Names() []string {
	return nil
}

func (p *Parameter) Value(ctx context.Context, _ reflect.Type, name string) (interface{}, bool, error) {
	parameter, ok := p.Parameters[name]
	if !ok {
		return nil, false, fmt.Errorf("uknonw parameter: %s", name)
	}
	return p.ParameterLookup(ctx, parameter)
}

// NewParameter returns parameter locator
func NewParameter(opts ...Option) (kind.Locator, error) {
	options := NewOptions(opts)
	if options.ParameterLookup == nil {
		return nil, fmt.Errorf("parameterLookup was empty")
	}
	if options.InputParameters == nil {
		return nil, fmt.Errorf("parameters was empty")
	}
	return &Parameter{
		ParameterLookup: options.ParameterLookup,
		Parameters:      options.InputParameters,
	}, nil
}
