// Package readerlookup carries the compiled route identity across the native
// handler/reader boundary. Nested handlers replace this invocation-local value.
package readerlookup

import (
	"context"
	"github.com/viant/datly/spec"
)

type key struct{}

func WithRoute(ctx context.Context, route spec.RouteRef) context.Context {
	return context.WithValue(ctx, key{}, route)
}
func Route(ctx context.Context) spec.RouteRef {
	route, _ := ctx.Value(key{}).(spec.RouteRef)
	return route
}
