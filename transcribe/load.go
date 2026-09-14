package transcribe

import (
	"fmt"
	"strings"

	"github.com/viant/datly/spec"
)

// componentLoader adapts the original load stage to Datly 1.0's canonical
// model. It merges metadata only; runtime views and services are compiled later.
type componentLoader struct {
	packageComponent  *spec.Component
	authoredComponent *spec.Component
}

func (l *componentLoader) Load() (*spec.Component, error) {
	base := l.packageComponent.Clone()
	authored := l.authoredComponent.Clone()
	if base == nil {
		if authored == nil {
			return nil, nil
		}
		views, err := l.mergeViews(nil, authored.Views)
		if err != nil {
			return nil, fmt.Errorf("load component views: %w", err)
		}
		authored.Views = views
		params, err := l.mergeParams(nil, authored.Parameters)
		if err != nil {
			return nil, fmt.Errorf("load component params: %w", err)
		}
		authored.Parameters = params
		return authored, nil
	}
	if authored == nil {
		views, err := l.mergeViews(base.Views, nil)
		if err != nil {
			return nil, fmt.Errorf("load component views: %w", err)
		}
		base.Views = views
		params, err := l.mergeParams(base.Parameters, nil)
		if err != nil {
			return nil, fmt.Errorf("load component params: %w", err)
		}
		base.Parameters = params
		return base, nil
	}
	packageScope := base.Key.Scope
	l.mergeIdentity(base, authored)
	base.Documentation = base.Documentation.Overlay(authored.Documentation)
	base.Settings = (&settingsLoader{base: base.Settings, authored: authored.Settings}).Load()
	base.TypeContext = l.mergeTypeContext(base.TypeContext, authored.TypeContext, packageScope)
	if len(authored.Routes) > 0 {
		routes, err := l.mergeRoutes(base.Routes, authored.Routes)
		if err != nil {
			return nil, fmt.Errorf("load component routes: %w", err)
		}
		base.Routes = routes
	}
	params, err := l.mergeParams(base.Parameters, authored.Parameters)
	if err != nil {
		return nil, fmt.Errorf("load component params: %w", err)
	}
	base.Parameters = params
	if authored.RootView != nil {
		base.RootView = authored.RootView
	}
	views, err := l.mergeViews(base.Views, authored.Views)
	if err != nil {
		return nil, fmt.Errorf("load component views: %w", err)
	}
	base.Views = views
	return base, nil
}

func (l *componentLoader) mergeRoutes(base, authored []*spec.Route) ([]*spec.Route, error) {
	handler := ""
	marshaller := ""
	baseByIdentity := make(map[string]*spec.Route, len(base))
	for _, route := range base {
		if route == nil {
			continue
		}
		if candidate := strings.TrimSpace(route.Handler); candidate != "" {
			if handler != "" && handler != candidate {
				return nil, fmt.Errorf("package routes declare conflicting handlers %q and %q", handler, candidate)
			}
			handler = candidate
		}
		if candidate := strings.TrimSpace(route.Marshaller); candidate != "" {
			if marshaller != "" && marshaller != candidate {
				return nil, fmt.Errorf("package routes declare conflicting marshallers %q and %q", marshaller, candidate)
			}
			marshaller = candidate
		}
		identity := (spec.RouteRef{Method: route.Method, Path: route.Path}).String()
		if identity != "" {
			baseByIdentity[identity] = route
		}
	}
	result := make([]*spec.Route, len(authored))
	for index, route := range authored {
		if route == nil {
			continue
		}
		item := route.Clone()
		if strings.TrimSpace(item.Handler) == "" {
			item.Handler = handler
		}
		if strings.TrimSpace(item.Marshaller) == "" {
			item.Marshaller = marshaller
		}
		identity := (spec.RouteRef{Method: item.Method, Path: item.Path}).String()
		if len(item.MCP) == 0 {
			if inherited := baseByIdentity[identity]; inherited != nil {
				inherited = inherited.Clone()
				item.MCP = inherited.MCP
			}
		}
		result[index] = item
	}
	return result, nil
}

func (l *componentLoader) mergeIdentity(base, authored *spec.Component) {
	if authored.Key.Kind != "" {
		base.Key.Kind = authored.Key.Kind
	}
	if strings.TrimSpace(authored.Key.Scope) != "" {
		base.Key.Scope = authored.Key.Scope
	}
	if strings.TrimSpace(authored.Key.Name) != "" {
		base.Key.Name = authored.Key.Name
	}
	if strings.TrimSpace(authored.Name) != "" {
		base.Name = authored.Name
	}
	if strings.TrimSpace(authored.Description) != "" {
		base.Description = authored.Description
	}
	if strings.TrimSpace(authored.Example) != "" {
		base.Example = authored.Example
	}
}
