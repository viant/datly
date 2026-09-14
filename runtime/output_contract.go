package runtime

import "github.com/viant/datly/runtime/registry"

// OutputByRoute uses the same route precedence as execution and authorization.
func (r *Runtime) OutputByRoute(method, path string) (*registry.OutputContract, bool) {
	if r == nil || r.bundle == nil {
		return nil, false
	}
	component, _, ok := r.publicComponentByRoute(method, path)
	if !ok || component == nil {
		return nil, false
	}
	registered := r.registered[component.Key.String()]
	if registered == nil {
		return nil, false
	}
	return registered.Output, registered.Output != nil
}
