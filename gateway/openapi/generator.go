// Package openapi generates OpenAPI 3.0.1 from compiled Datly registrations.
package openapi

import (
	"context"
	"fmt"
	"strings"

	"github.com/viant/datly/gateway/openapi/openapi3"
	"github.com/viant/datly/runtime/registry"

	"github.com/viant/datly/spec"
)

// Visibility is implemented by runtime.Runtime. Supply the serving runtime to
// apply its package exposure policy to the supplied registration catalog.
type Visibility interface {
	RouteByMethodPath(method, path string) (*spec.Route, bool)
}

type Request struct {
	Info       openapi3.Info
	Components []*registry.RegisteredComponent
	Visibility Visibility
	// Routes optionally restricts publication to exact canonical routes, while
	// the full Components catalog remains available for private dependencies.
	Routes []spec.RouteRef
}

// Generator has no mutable state; every document owns its schemas and operations.
type Generator struct{}

func (Generator) Generate(ctx context.Context, request Request) (*openapi3.OpenAPI, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if strings.TrimSpace(request.Info.Title) == "" || strings.TrimSpace(request.Info.Version) == "" {
		return nil, fmt.Errorf("OpenAPI info requires title and version")
	}
	if request.Info.License != nil && strings.TrimSpace(request.Info.License.Name) == "" {
		return nil, fmt.Errorf("OpenAPI license requires a name")
	}

	for _, entry := range request.Components {
		if entry == nil || entry.Component == nil || entry.Input == nil || entry.Output == nil {
			return nil, fmt.Errorf("OpenAPI requires complete compiled component registrations")
		}
		if entry.Output.Type() == nil || entry.Output.Type() != entry.OutputType {
			return nil, fmt.Errorf("component %s: compiled output type is missing or inconsistent", entry.Component.Key.String())
		}
	}
	catalog, err := registry.NewInputCatalog(request.Components)
	if err != nil {
		return nil, err
	}
	selected := map[string]bool{}
	for _, ref := range request.Routes {
		if selected[ref.String()] {
			return nil, fmt.Errorf("duplicate selected route %s", ref.String())
		}
		if _, err := catalog.Fields(ref); err != nil {
			return nil, err
		}
		selected[ref.String()] = true
	}
	info := request.Info
	if info.Contact != nil {
		value := *info.Contact
		info.Contact = &value
	}
	if info.License != nil {
		value := *info.License
		info.License = &value
	}
	doc := &openapi3.OpenAPI{OpenAPI: "3.0.1", Info: &info, Paths: openapi3.Paths{}, Components: openapi3.Components{
		Schemas: openapi3.Schemas{}, SecuritySchemes: openapi3.SecuritySchemes{},
	}}
	paths := pathBuilder{inputs: catalog, document: doc, schemas: newSchemaBuilder(doc.Components.Schemas), shapes: map[string]string{}, operations: map[string]bool{}}
	for _, entry := range request.Components {
		for _, endpoint := range entry.Component.Routes {
			if err := ctx.Err(); err != nil {
				return nil, err
			}
			if endpoint == nil {
				return nil, fmt.Errorf("component %s has a nil route", entry.Component.Key.String())
			}
			if request.Routes != nil && !selected[(spec.RouteRef{Method: endpoint.Method, Path: endpoint.Path}).String()] {
				continue
			}
			if request.Visibility != nil {
				exposed, ok := request.Visibility.RouteByMethodPath(endpoint.Method, endpoint.Path)
				if !ok {
					continue
				}
				if exposed == nil || exposed.Method != endpoint.Method || exposed.Path != endpoint.Path ||
					exposed.Marshaller != endpoint.Marshaller || exposed.APIKeyHeader != endpoint.APIKeyHeader || exposed.APIKeyValue != endpoint.APIKeyValue {
					return nil, fmt.Errorf("visibility catalog disagrees with route %s %s", endpoint.Method, endpoint.Path)
				}
			}
			if err := paths.add(entry, endpoint); err != nil {
				return nil, fmt.Errorf("OpenAPI %s %s: %w", endpoint.Method, endpoint.Path, err)
			}
		}
	}
	return doc, nil
}
