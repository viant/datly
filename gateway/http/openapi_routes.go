package http

import (
	"context"
	"fmt"
	"net/url"
	"strings"

	"github.com/viant/datly/gateway/openapi"
	"github.com/viant/datly/runtime/route"
	"github.com/viant/datly/spec"
)

func (c Config) documents(ctx context.Context, input HandlerInput) (*documentRoutes, error) {
	if c.OpenAPI == nil || strings.TrimSpace(c.Meta.OpenApiURI) == "" {
		return nil, nil
	}
	if input.Components == nil {
		return nil, fmt.Errorf("OpenAPI staging requires canonical Build registrations")
	}
	if err := c.OpenAPI.AggregateAccess.Validate(); err != nil {
		return nil, err
	}
	if err := c.OpenAPI.RouteAccess.Validate(); err != nil {
		return nil, err
	}
	result := &documentRoutes{prefix: strings.TrimSuffix(c.Meta.OpenApiURI, "/"), apiPrefix: strings.TrimSuffix(c.APIPrefix, "/"), uiPath: strings.TrimSpace(c.Meta.DocURI), paths: map[string]*documentRoute{}}
	for _, path := range []string{result.prefix, result.apiPrefix, result.uiPath} {
		if path == "" {
			continue
		}
		parsed, err := url.Parse(path)
		if err != nil || !strings.HasPrefix(path, "/") || parsed.RawQuery != "" || parsed.Fragment != "" || strings.ContainsAny(path, "{}%*\r\n ") {
			return nil, fmt.Errorf("invalid OpenAPI HTTP prefix %q", path)
		}
	}
	if result.prefix == "" || result.prefix == result.apiPrefix {
		return nil, fmt.Errorf("OpenAPI prefix must be distinct")
	}
	if result.uiPath != "" && (result.uiPath == result.prefix || strings.HasPrefix(result.uiPath, result.prefix+"/")) {
		return nil, fmt.Errorf("DocURI collides with OpenAPI namespace")
	}
	warm := strings.TrimSuffix(strings.TrimSpace(c.Meta.CacheWarmURI), "/")
	for _, path := range []string{result.prefix, result.uiPath} {
		if path != "" && warm != "" && (path == warm || strings.HasPrefix(path, warm+"/") || strings.HasPrefix(warm, path+"/")) {
			return nil, fmt.Errorf("OpenAPI URI collides with warmup namespace")
		}
	}
	var specs []*spec.Component
	for _, entry := range input.Components {
		if entry == nil || entry.Component == nil {
			return nil, fmt.Errorf("OpenAPI requires compiled components")
		}
		specs = append(specs, entry.Component)
	}
	all, err := route.NewBundle(specs)
	if err != nil {
		return nil, err
	}
	groups := map[string][]spec.RouteRef{}
	for _, endpoint := range input.Runtime.Routes() {
		if result.apiPrefix != "" && endpoint.Path != result.apiPrefix && !strings.HasPrefix(endpoint.Path, result.apiPrefix+"/") {
			continue
		}
		groups[endpoint.Path] = append(groups[endpoint.Path], spec.RouteRef{Method: endpoint.Method, Path: endpoint.Path})
	}
	metadataPaths := []string{result.prefix}
	if result.uiPath != "" {
		metadataPaths = append(metadataPaths, result.uiPath)
	}
	for path := range groups {
		target := result.prefix + strings.TrimPrefix(path, result.apiPrefix)
		if target == result.prefix {
			return nil, fmt.Errorf("per-path OpenAPI route collides with aggregate")
		}
		metadataPaths = append(metadataPaths, target)
	}
	for _, component := range specs {
		for _, endpoint := range component.Routes {
			if endpoint == nil {
				return nil, fmt.Errorf("OpenAPI component has a nil route")
			}
			if endpoint.Path == result.prefix || strings.HasPrefix(endpoint.Path, result.prefix+"/") {
				return nil, fmt.Errorf("OpenAPI namespace collides with component route %s", endpoint.Path)
			}
		}
	}
	for _, path := range metadataPaths {
		if len(all.AllowedMethodsForPath(path)) > 0 {
			return nil, fmt.Errorf("OpenAPI metadata route collides with component route %s", path)
		}
	}
	result.cors, err = newCORSPolicy(c.CORS)
	if err != nil {
		return nil, err
	}
	request := openapi.Request{Info: c.OpenAPI.Info, Components: input.Components, Visibility: input.Runtime}
	// Apply APIPrefix to the aggregate as original router assembly did.
	request.Routes = []spec.RouteRef{}
	for _, refs := range groups {
		request.Routes = append(request.Routes, refs...)
	}
	result.aggregate, err = result.compile(ctx, request, c.OpenAPI.AggregateAccess, true)
	if err != nil {
		return nil, err
	}
	for path, refs := range groups {
		request.Routes = refs
		result.paths[path], err = result.compile(ctx, request, c.OpenAPI.RouteAccess, false)
		if err != nil {
			return nil, err
		}
	}
	if result.uiPath != "" {
		result.ui, err = result.uiDocument()
		if err != nil {
			return nil, err
		}
	}
	return result, nil
}

func (d *documentRoutes) compile(ctx context.Context, request openapi.Request, access *DocumentAccess, aggregate bool) (*documentRoute, error) {
	snapshot, err := openapi.NewSnapshot(ctx, request)
	if err != nil {
		return nil, err
	}
	options := openapi.ServeOptions{DefaultFormat: "yaml"}
	if aggregate {
		options = openapi.ServeOptions{DefaultFormat: "json", Negotiate: true}
	}
	handler, err := snapshot.Handler(options)
	if err != nil {
		return nil, err
	}
	result := &documentRoute{snapshot: snapshot, handler: handler}
	if access != nil {
		result.access = *access
	}
	return result, nil
}
