package runtime

import (
	"github.com/viant/datly/runtime/route"
	"github.com/viant/datly/spec"
)

func (r *Runtime) publicRoutes() *route.Bundle {
	if r == nil {
		return nil
	}
	if r.publicBundle != nil {
		return r.publicBundle
	}
	return r.bundle
}

// ExposesComponent is the protocol catalog boundary. Internal invocation uses
// the complete registry and does not consult endpoint exposure.
func (r *Runtime) ExposesComponent(key spec.Key) bool {
	if r == nil || r.publicRoutes() == nil {
		return false
	}
	_, ok := r.publicRoutes().ComponentByKey(key)
	if ok {
		return true
	}
	_, ok = r.relatedExposure.Load(key.String())
	return ok
}

// Resolve against the full route catalog before applying visibility. A hidden
// exact route must not fall through to a public path-template component.
func (r *Runtime) publicComponentByRoute(method, path string) (*spec.Component, map[string]string, bool) {
	if r == nil || r.bundle == nil {
		return nil, nil, false
	}
	component, params, ok := r.bundle.ComponentByRouteWithParams(method, path)
	if !ok || component == nil || !r.ExposesComponent(component.Key) {
		return nil, nil, false
	}
	return component, params, true
}
