package openapi

import (
	"context"
	"fmt"
	openapi "github.com/viant/datly/gateway/router/openapi/openapi3"
	"github.com/viant/datly/repository"
	"net/http"
)

func (g *generator) generatePaths(ctx context.Context, components *repository.Service, providers []*repository.Provider) (*SchemaContainer, openapi.Paths, error) {
	container := NewContainer()
	builder := &PathsBuilder{paths: openapi.Paths{}}
	var retErr error

	for _, provider := range providers {
		component, err := provider.Component(ctx)
		if err != nil {
			retErr = err
		}
		if component == nil {
			fmt.Printf("provider.Component(ctx) returned nil\n")
			continue
		}

		componentSchema := NewComponentSchema(components, component, container)
		operation, err := g.generateOperation(ctx, componentSchema)
		if err != nil {
			retErr = err
		}

		pathItem := &openapi.PathItem{}
		attachOperation(pathItem, component.Method, operation)
		builder.AddPath(component.URI, pathItem)
	}

	return container, builder.paths, retErr
}

func attachOperation(pathItem *openapi.PathItem, method string, operation *openapi.Operation) {
	switch method {
	case http.MethodGet:
		pathItem.Get = operation
	case http.MethodPost:
		pathItem.Post = operation
	case http.MethodDelete:
		pathItem.Delete = operation
	case http.MethodPut:
		pathItem.Put = operation
	case http.MethodPatch:
		pathItem.Patch = operation
	}
}
