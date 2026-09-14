package runtime

import (
	dexec "github.com/viant/datly/exec"
	"github.com/viant/datly/spec"
	"sort"
)

// Routes returns detached public route metadata for protocol configuration.
func (r *Runtime) Routes() []*spec.Route {
	var result []*spec.Route
	if r == nil {
		return result
	}
	for _, entry := range r.registered {
		if !r.ExposesComponent(entry.Component.Key) {
			continue
		}
		for _, endpoint := range entry.Component.Routes {
			if endpoint != nil {
				result = append(result, endpoint.Clone())
			}
		}
	}
	sort.Slice(result, func(i, j int) bool {
		return (spec.RouteRef{Method: result[i].Method, Path: result[i].Path}).String() < (spec.RouteRef{Method: result[j].Method, Path: result[j].Path}).String()
	})
	return result
}

// WarmupTarget resolves public GET visibility before returning an exact eligible
// reader target. It uses the same route resolution as ordinary HTTP execution.
func (r *Runtime) WarmupTarget(path string) (dexec.ComponentTarget, bool) {
	component, _, ok := r.publicComponentByRoute("GET", path)
	if !ok {
		return dexec.ComponentTarget{}, false
	}
	endpoint, ok := r.RouteByMethodPath("GET", path)
	if !ok {
		return dexec.ComponentTarget{}, false
	}
	if component.Settings == nil || component.Settings.Cache == nil || !component.Settings.Cache.Enabled {
		return dexec.ComponentTarget{}, false
	}
	target := dexec.ComponentTarget{Component: component.Key, Route: spec.RouteRef{Method: endpoint.Method, Path: endpoint.Path}}
	_, err := r.NewWarmup(target)
	return target, err == nil
}
