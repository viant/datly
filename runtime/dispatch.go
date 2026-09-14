package runtime

import (
	"context"
	"fmt"

	dexec "github.com/viant/datly/exec"
	"github.com/viant/datly/spec"
)

func (r *Runtime) executeRoute(ctx context.Context, method, path string, scope dexec.ProviderScope) (any, error) {
	if r == nil || r.bundle == nil || r.invoker == nil {
		return nil, fmt.Errorf("runtime route dispatch is not configured")
	}
	if method == "" || path == "" {
		return nil, fmt.Errorf("route method and path are required")
	}
	component, _, ok := r.publicComponentByRoute(method, path)
	if !ok {
		return nil, fmt.Errorf("route not found: %s %s", method, path)
	}
	if component == nil {
		return nil, fmt.Errorf("route component missing for %s %s", method, path)
	}
	route, ok := r.bundle.RouteByMethodPath(method, path)
	if !ok || route == nil {
		return nil, fmt.Errorf("route metadata not found: %s %s", method, path)
	}
	return r.invokeComponent(ctx, dexec.ComponentRequest{
		Target: dexec.ComponentTarget{
			Component: component.Key,
			Route:     spec.RouteRef{Method: route.Method, Path: route.Path},
		},
	}, scope)
}
