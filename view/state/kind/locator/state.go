package locator

import (
	"context"
	"fmt"
	"github.com/viant/datly/view/state/kind"
	"github.com/viant/structology"
)

type State struct {
	State *structology.State
}

func (p *State) Names() []string {
	return nil
}
func (p *State) Value(ctx context.Context, name string) (interface{}, bool, error) {
	_, err := p.State.Selector(name)
	if err != nil {
		return nil, false, nil
	}
	value, err := p.State.Value(name)
	return value, err == nil, err
}

// NewState returns state locator
func NewState(opts ...Option) (kind.Locator, error) {
	options := NewOptions(opts)
	if options.State == nil {
		return nil, fmt.Errorf("state was empty")
	}
	return &State{
		State: options.State,
	}, nil
}
