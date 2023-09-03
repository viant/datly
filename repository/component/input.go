package component

import (
	"context"
	"fmt"
	"github.com/viant/datly/view"
	"github.com/viant/datly/view/state"
)

type Input struct {
	Body             state.Type
	Parameters       state.NamedParameters
	CustomValidation bool `json:",omitempty"`
}

func (i *Input) Init(ctx context.Context, aView *view.View) error {
	if len(i.Body.Parameters) == 0 {
		i.Body.Parameters = aView.InputParameters()
	}
	if err := i.Body.Init(state.WithResource(aView.Resource()),
		state.WithPackage(pkgPath),
		state.WithMarker(true),
		state.WithBodyType(true)); err != nil {
		return fmt.Errorf("failed to initialise input: %w", err)
	}
	return nil
}
