package runtime

import (
	"context"
	"fmt"
	"sort"
	"strings"

	dexec "github.com/viant/datly/exec"
	"github.com/viant/datly/spec"
)

// CacheTarget resolves a server-owned GET component, including lazy-only views.
func (r *Runtime) CacheTarget(path string) (dexec.ComponentTarget, bool) {
	if r == nil || r.bundle == nil {
		return dexec.ComponentTarget{}, false
	}
	component, _, ok := r.bundle.ComponentByRouteWithParams("GET", path)
	if !ok || component == nil || (r.exposure != nil && !r.exposure.Allows(component.Key.Scope)) {
		return dexec.ComponentTarget{}, false
	}
	route, ok := r.bundle.RouteByMethodPath("GET", path)
	if !ok {
		return dexec.ComponentTarget{}, false
	}
	target := dexec.ComponentTarget{Component: component.Key, Route: spec.RouteRef{Method: route.Method, Path: route.Path}}
	if component.Settings != nil && component.Settings.WarmupTarget != nil {
		ref := *component.Settings.WarmupTarget
		delegated, _, found := r.bundle.ComponentByRouteWithParams(ref.Method, ref.Path)
		if !found || delegated == nil || r.exposure != nil && !r.exposure.Allows(delegated.Key.Scope) {
			return dexec.ComponentTarget{}, false
		}
		target = dexec.ComponentTarget{Component: delegated.Key, Route: ref}
	}
	manager, err := r.cacheManager(target)
	return target, err == nil && len(manager.CacheViews()) > 0
}
func (r *Runtime) CacheRoutes() []*spec.Route {
	var result []*spec.Route
	for _, component := range r.metadata {
		if component == nil || (r.exposure != nil && !r.exposure.Allows(component.Key.Scope)) {
			continue
		}
		for _, route := range component.Routes {
			if route != nil && strings.EqualFold(route.Method, "GET") {
				if _, ok := r.CacheTarget(route.Path); ok {
					result = append(result, route.Clone())
				}
			}
		}
	}
	sort.Slice(result, func(i, j int) bool { return result[i].Path < result[j].Path })
	return result
}
func (r *Runtime) cacheManager(target dexec.ComponentTarget) (dexec.ReaderCacheManager, error) {
	if r == nil {
		return nil, fmt.Errorf("runtime is required")
	}
	registered := r.registered[target.Component.String()]
	if registered == nil || registered.Component == nil || !componentOwnsRoute(registered.Component, target.Route) {
		return nil, fmt.Errorf("cache component not found")
	}
	manager, ok := registered.Reader.(dexec.ReaderCacheManager)
	if !ok {
		return nil, fmt.Errorf("reader does not support cache administration")
	}
	return manager, nil
}
func (r *Runtime) InvalidateCache(ctx context.Context, target dexec.ComponentTarget, view, scope string) ([]dexec.CacheInvalidation, error) {
	manager, err := r.cacheManager(target)
	if err != nil {
		return nil, err
	}
	return manager.InvalidateCache(ctx, view, scope)
}

// CacheRoute preserves the requesting route's policy even when it delegates
// cache administration to a reader shared by multiple public routes.
func (r *Runtime) CacheRoute(path string) (spec.RouteRef, bool) {
	if _, ok := r.CacheTarget(path); !ok {
		return spec.RouteRef{}, false
	}
	route, ok := r.bundle.RouteByMethodPath("GET", path)
	if !ok {
		return spec.RouteRef{}, false
	}
	return spec.RouteRef{Method: route.Method, Path: route.Path}, true
}
