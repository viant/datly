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
		if bodyParameter := i.Type.Parameters.LookupByLocation(state.KindRequestBody, ""); bodyParameter != nil {
			i.Body.Parameters = append(i.Body.Parameters, bodyParameter)
		} else if bodyParameters := i.Type.Parameters.FilterByKind(state.KindRequestBody); len(bodyParameters) > 0 {
			i.Body.Parameters = bodyParameters
		} else {
			viewParameters := aView.InputParameters()
			for j, candidate := range viewParameters {
				if candidate.In.Kind == state.KindRequest {
					continue
				}
				i.Body.Parameters = append(i.Body.Parameters, viewParameters[j])
			}
		}
	}

	if len(i.Body.Parameters) == 0 {
		i.Body.Parameters = i.Type.Parameters.FilterByKind(state.KindRequestBody)
		for _, candidate := range i.Body.Parameters {
			if candidate.In.Name == "" {
				i.Body.Schema = candidate.Schema.Clone()
				break
			}
		}
	}

	pkg := pkgPath
	if i.Type.Schema != nil && i.Type.Package != "" {
		pkg = i.Type.Package
	}

	if err := i.Body.Init(state.WithResource(aView.Resource()),
		state.WithPackage(pkg),
		state.WithMarker(true),
		state.WithBodyType(true)); err != nil {
		return fmt.Errorf("failed to initialise input: %w", err)
	}

	if i.Body.Type() != nil {
		bodyType := i.Body.Type().Type()
		if bodyParameter := i.Type.Parameters.LookupByLocation(state.KindRequestBody, ""); bodyParameter != nil {
			bodyParameter.Schema.SetType(bodyType)
		}
	}

	resourcelet := aView.Resource()

	for _, parameter := range i.Type.Parameters {
		if err := parameter.Init(ctx, resourcelet); err != nil {
			return err
		}
	}
	if err := i.Type.Init(state.WithResource(aView.Resource()),
		state.WithPackage(pkg),
		state.WithMarker(true),
		state.WithBodyType(false)); err != nil {
		return fmt.Errorf("failed to initialise input: %w", err)
	}

	return nil
}
