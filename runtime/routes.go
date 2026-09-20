package runtime

import (
	"sort"
	"strings"

	dexec "github.com/viant/datly/exec"
	"github.com/viant/datly/spec"
)

// Routes returns detached public route metadata for protocol configuration.
func (r *Runtime) Routes() []*spec.Route {
	var result []*spec.Route
	if r == nil {
		return result
	}
	for _, component := range r.metadata {
		if !r.ExposesComponent(component.Key) {
			continue
		}
		for _, endpoint := range component.Routes {
			if spec.PublicRoute(endpoint) {
				result = append(result, endpoint.Clone())
			}
		}
	}
	sort.Slice(result, func(i, j int) bool {
		return (spec.RouteRef{Method: result[i].Method, Path: result[i].Path}).String() < (spec.RouteRef{Method: result[j].Method, Path: result[j].Path}).String()
	})
	return result
}

// WarmupTarget resolves exact GET warmup eligibility without requiring public
// HTTP visibility. Warmup is separately administrator-authorized.
func (r *Runtime) WarmupTarget(path string) (dexec.ComponentTarget, bool) {
	if r == nil || r.bundle == nil {
		return dexec.ComponentTarget{}, false
	}
	component, _, ok := r.bundle.ComponentByRouteWithParams("GET", path)
	if !ok {
		return dexec.ComponentTarget{}, false
	}
	if r.exposure != nil && !r.exposure.Allows(component.Key.Scope) {
		return dexec.ComponentTarget{}, false
	}
	endpoint, ok := r.bundle.RouteByMethodPath("GET", path)
	if !ok {
		return dexec.ComponentTarget{}, false
	}
	target := dexec.ComponentTarget{Component: component.Key, Route: spec.RouteRef{Method: endpoint.Method, Path: endpoint.Path}}
	_, err := r.NewWarmup(target)
	return target, err == nil
}

// WarmupRoutes returns detached GET route metadata for operational cache
// warmup. Unlike Routes, this includes internal reader routes because warmup is
// separately administrator-authorized and does not publish the component.
func (r *Runtime) WarmupRoutes() []*spec.Route {
	var result []*spec.Route
	if r == nil {
		return result
	}
	for _, component := range r.metadata {
		if component == nil || r.exposure != nil && !r.exposure.Allows(component.Key.Scope) {
			continue
		}
		for _, endpoint := range component.Routes {
			if endpoint != nil && strings.EqualFold(endpoint.Method, "GET") {
				if _, ok := r.WarmupTarget(endpoint.Path); ok {
					result = append(result, endpoint.Clone())
				}
			}
		}
	}
	sort.Slice(result, func(i, j int) bool {
		return (spec.RouteRef{Method: result[i].Method, Path: result[i].Path}).String() < (spec.RouteRef{Method: result[j].Method, Path: result[j].Path}).String()
	})
	return result
}
