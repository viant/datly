package openapi

import (
	"context"
	openapi "github.com/viant/datly/gateway/router/openapi/openapi3"
	"github.com/viant/datly/repository"
	"github.com/viant/datly/repository/contract"
	"github.com/viant/datly/shared"
	"github.com/viant/datly/view/state"
)

func (g *generator) generateOperation(ctx context.Context, component *ComponentSchema) (*openapi.Operation, error) {
	body, err := g.requestBody(ctx, component)
	if err != nil {
		return nil, err
	}

	parameters, err := g.operationParameters(ctx, component)
	if err != nil {
		return nil, err
	}

	responses, err := g.responses(ctx, component)
	if err != nil {
		return nil, err
	}

	return &openapi.Operation{
		Parameters:  dedupe(parameters),
		RequestBody: body,
		Responses:   responses,
	}, nil
}

func (g *generator) operationParameters(ctx context.Context, component *ComponentSchema) ([]*openapi.Parameter, error) {
	parameters, err := g.getAllViewsParameters(ctx, component, component.component.View)
	if err != nil {
		return nil, err
	}

	componentParams, err := g.componentOutputParameters(ctx, component)
	if err != nil {
		return nil, err
	}
	return append(parameters, componentParams...), nil
}

func (g *generator) componentOutputParameters(ctx context.Context, component *ComponentSchema) ([]*openapi.Parameter, error) {
	result := make([]*openapi.Parameter, 0)
	err := g.forEachParam(component.component.Output.Type.Parameters, func(parameter *state.Parameter) (bool, error) {
		if parameter.In.Kind != state.KindComponent {
			return true, nil
		}

		paramComponent, err := g.lookupComponentParam(ctx, component, parameter.In.Name)
		if err != nil {
			return false, err
		}

		viewsParameters, err := g.getAllViewsParameters(ctx, NewComponentSchema(component.components, paramComponent, component.schemas), paramComponent.View)
		if err != nil {
			return false, err
		}

		result = append(result, viewsParameters...)
		return true, nil
	})
	if err != nil {
		return nil, err
	}
	return result, nil
}

func (g *generator) lookupComponentParam(ctx context.Context, component *ComponentSchema, path string) (*repository.Component, error) {
	method, URI := shared.ExtractPath(path)
	provider, err := component.components.Registry().LookupProvider(ctx, &contract.Path{URI: URI, Method: method})
	if err != nil {
		return nil, err
	}
	return provider.Component(ctx)
}
