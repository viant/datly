package contract

import (
	"context"
	"fmt"
	"github.com/viant/datly/view"
	"github.com/viant/datly/view/state"
)

type Input struct {
	Body                       state.Type
	Type                       state.Type
	CustomValidation           bool `json:",omitempty"`
	IgnoreEmptyQueryParameters bool `json:",omitempty"`
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

	bodyType := i.Body.Type().Type()
	if bodyParam := i.Type.Parameters.LookupByLocation(state.KindRequestBody, ""); bodyParam != nil {
		bodyParam.Schema.SetType(bodyType)
	}

	resourcelet := aView.Resource()
	for _, parameter := range i.Type.Parameters {
		if err := parameter.Init(ctx, resourcelet); err != nil {
			return err
		}
	}
	if err := i.Type.Init(state.WithResource(aView.Resource()),
		state.WithPackage(pkgPath),
		state.WithMarker(true),
		state.WithBodyType(false)); err != nil {
		return fmt.Errorf("failed to initialise input: %w", err)
	}

	return nil
}
