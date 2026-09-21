package dql

import (
	"errors"
	"fmt"
	"strings"

	"github.com/viant/datly/spec"
)

// ErrMissingRouteDirective reports a source with no #setting route directive:
// valid SQL, but not a component source. Callers that scan mixed SQL assets
// (e.g. transcribe source discovery) classify non-component files with
// errors.Is against this sentinel instead of re-implementing detection.
var ErrMissingRouteDirective = errors.New("missing #setting route directive in source")

// parseComponentSource is the package-local convenience used by focused syntax
// tests. Production source orchestration enters through transcribe.Compiler.
func parseComponentSource(scope, name, source string) (*spec.Component, error) {
	prepared := PrepareSource(source)
	if err := prepared.Err(); err != nil {
		return nil, err
	}
	return ParsePreparedComponentSource(scope, name, prepared)
}

func ParsePreparedComponentSource(scope, name string, prepared *PreparedSource) (*spec.Component, error) {
	if prepared == nil {
		return nil, fmt.Errorf("prepared DQL source is required")
	}
	if err := prepared.Err(); err != nil {
		return nil, err
	}
	if prepared.Directives == nil {
		return nil, fmt.Errorf("normalized DQL directives are required")
	}
	route := prepared.Directives.Route
	if route == nil {
		return nil, ErrMissingRouteDirective
	}

	path := strings.TrimSpace(route.URI)
	methods := route.Methods
	if path == "" {
		return nil, fmt.Errorf("missing route metadata in source")
	}
	if static := prepared.Directives.Static; static != nil {
		runtimeSettings := prepared.Directives.Settings.Clone()
		if runtimeSettings != nil {
			runtimeSettings.Generation = nil
		}

		if strings.TrimSpace(prepared.SQL) != "" || len(prepared.Directives.Params) != 0 || len(prepared.Directives.Views) != 0 || prepared.Directives.MCP != nil || !runtimeSettings.IsZero() {
			return nil, fmt.Errorf("static declarations cannot contain SQL, handler, parameter or component settings")
		}
		for _, method := range methods {
			if method != "GET" && method != "HEAD" {
				return nil, fmt.Errorf("static routes support GET and HEAD")
			}
		}
		static = static.Clone()
		static.Path = path
		static.APIKeyHeader = route.APIKeyHeader
		static.APIKeyValue = route.APIKeyValue
		if err := static.Validate(); err != nil {
			return nil, err
		}
		return &spec.Component{Key: spec.Key{Kind: spec.KindComponent, Scope: scope, Name: name}, Name: name, Static: static, Settings: prepared.Directives.Settings.Clone()}, nil
	}
	viewSQL := prepared.SQL
	component := &spec.Component{
		Key:           spec.Key{Kind: spec.KindComponent, Scope: scope, Name: name},
		Name:          name,
		Settings:      prepared.Directives.Settings,
		Documentation: prepared.Directives.Documentation.Clone(),
		TypeContext:   prepared.TypeContext,
		Parameters:    prepared.Directives.Params,
		Views:         scopedViews(scope, prepared.Directives.Views),
		RootView: &spec.View{
			Key:  spec.Key{Kind: spec.KindView, Scope: scope, Name: name},
			Name: name,
			Source: &spec.ViewSource{
				SQL:    viewSQL,
				Embeds: EmbeddedSQLRefs(viewSQL),
			},
		},
	}
	for _, method := range methods {
		routeSpec := &spec.Route{
			Method:       strings.TrimSpace(method),
			Path:         path,
			Name:         name,
			APIKeyHeader: route.APIKeyHeader,
			APIKeyValue:  route.APIKeyValue,
			Internal:     prepared.Directives.MCPOnly,
		}
		if prepared.Directives.MCP != nil {
			routeSpec.MCP = []*spec.MCPExposure{prepared.Directives.MCP.Clone()}
		}
		component.Routes = append(component.Routes, routeSpec)
	}
	return component, nil
}

func scopedViews(scope string, source []*spec.View) []*spec.View {
	if len(source) == 0 {
		return nil
	}
	result := make([]*spec.View, 0, len(source))
	for _, view := range source {
		if view == nil {
			continue
		}
		cloned := view.Clone()
		cloned.Key.Kind = spec.KindView
		cloned.Key.Scope = scope
		if strings.TrimSpace(cloned.Key.Name) == "" {
			cloned.Key.Name = cloned.CanonicalName()
		}
		result = append(result, cloned)
	}
	return result
}
