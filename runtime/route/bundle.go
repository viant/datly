package route

import (
	"fmt"
	"strings"

	"github.com/viant/datly/spec"
)

type Bundle struct {
	components map[string]*spec.Component
	routes     map[string]*spec.Component
	routeMeta  map[string]*spec.Route
	routeDefs  []*def
}

type def struct {
	Method    string
	Path      string
	Template  *PathTemplate
	Route     *spec.Route
	Component *spec.Component
}

func NewBundle(components []*spec.Component) (*Bundle, error) {
	b := &Bundle{
		components: map[string]*spec.Component{},
		routes:     map[string]*spec.Component{},
		routeMeta:  map[string]*spec.Route{},
		routeDefs:  []*def{},
	}
	templateKeys := map[string]bool{}
	for _, component := range components {
		if component == nil {
			continue
		}
		key := component.Key.String()
		if _, exists := b.components[key]; exists {
			return nil, fmt.Errorf("duplicate component key %s", key)
		}
		b.components[key] = component
		for _, route := range component.Routes {
			if route == nil {
				continue
			}
			template, err := CompilePathTemplate(route.Path)
			if err != nil {
				return nil, fmt.Errorf("route %s %s: %w", route.Method, route.Path, err)
			}
			indexKey := routeIndexKey(route.Method, route.Path)
			if _, exists := b.routes[indexKey]; exists {
				return nil, fmt.Errorf("duplicate route %s", indexKey)
			}
			templateKey := routeTemplateIndexKey(route.Method, template)
			if templateKeys[templateKey] {
				return nil, fmt.Errorf("duplicate route template %s", templateKey)
			}
			b.routes[indexKey] = component
			b.routeMeta[indexKey] = route
			templateKeys[templateKey] = true
			b.routeDefs = append(b.routeDefs, &def{
				Method:    strings.ToUpper(strings.TrimSpace(route.Method)),
				Path:      strings.TrimSpace(route.Path),
				Template:  template,
				Route:     route,
				Component: component,
			})
		}
	}
	return b, nil
}

func (b *Bundle) ComponentByRoute(method, path string) (*spec.Component, bool) {
	component, _, ok := b.ComponentByRouteWithParams(method, path)
	return component, ok
}

func (b *Bundle) ComponentByKey(key spec.Key) (*spec.Component, bool) {
	if b == nil {
		return nil, false
	}
	component, ok := b.components[key.String()]
	return component, ok
}

func (b *Bundle) ComponentByRouteWithParams(method, path string) (*spec.Component, map[string]string, bool) {
	if b == nil {
		return nil, nil, false
	}
	component, ok := b.routes[routeIndexKey(method, path)]
	if ok {
		return component, map[string]string{}, true
	}
	method = strings.ToUpper(strings.TrimSpace(method))
	path = strings.TrimSpace(path)
	for _, route := range b.routeDefs {
		if route == nil || route.Component == nil || route.Method != method {
			continue
		}
		if params, ok, err := route.Template.MatchEscapedPath(path); err == nil && ok {
			return route.Component, params, true
		}
	}
	return nil, nil, false
}

func (b *Bundle) RouteByMethodPath(method, path string) (*spec.Route, bool) {
	if b == nil {
		return nil, false
	}
	if route, ok := b.routeMeta[routeIndexKey(method, path)]; ok {
		return route, true
	}
	method = strings.ToUpper(strings.TrimSpace(method))
	path = strings.TrimSpace(path)
	for _, route := range b.routeDefs {
		if route == nil || route.Route == nil || route.Method != method {
			continue
		}
		if _, ok, err := route.Template.MatchEscapedPath(path); err == nil && ok {
			return route.Route, true
		}
	}
	return nil, false
}

func (b *Bundle) AllowedMethodsForPath(path string) []string {
	if b == nil {
		return nil
	}
	path = strings.TrimSpace(path)
	seen := map[string]bool{}
	var result []string
	for _, route := range b.routeDefs {
		if route == nil || route.Route == nil {
			continue
		}
		if _, ok, err := route.Template.MatchEscapedPath(path); err != nil || !ok {
			continue
		}
		method := strings.ToUpper(strings.TrimSpace(route.Method))
		if method == "" || seen[method] {
			continue
		}
		seen[method] = true
		result = append(result, method)
	}
	return result
}
