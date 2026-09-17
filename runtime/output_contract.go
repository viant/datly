package runtime

import (
	"context"
	"fmt"

	"github.com/viant/datly/runtime/output"
	"github.com/viant/datly/runtime/registry"
)

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

// ResolveOutputByRoute resolves output authority through this generation's
// loader. HTTP must not serialize a lazy component without its output contract.
func (r *Runtime) ResolveOutputByRoute(ctx context.Context, method, path string) (*registry.OutputContract, error) {
	if ctx == nil {
		return nil, fmt.Errorf("output contract context is required")
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if r == nil || r.bundle == nil {
		return nil, fmt.Errorf("runtime is not configured")
	}
	component, _, ok := r.publicComponentByRoute(method, path)
	if !ok || component == nil {
		return nil, fmt.Errorf("output route not found: %s %s", method, path)
	}
	registered, err := r.registeredComponent(ctx, component.Key)
	if err != nil {
		return nil, err
	}
	if registered.Output != nil {
		return registered.Output, nil
	}
	// Match NewRuntime's support for registrations with an implicit output plan,
	// without mutating generation-owned registrations during concurrent requests.
	return (output.Compiler{}).Compile(output.CompileInput{Component: registered.Component, Type: registered.OutputType})
}
