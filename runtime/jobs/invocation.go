package jobs

import (
	"context"
	xasync "github.com/viant/xdatly/async"
)

// Invocation carries the original event-invocation metadata in a fresh replay
// scope. It is execution metadata, not a global or client-bindable principal.
type Invocation struct {
	Job  *xasync.Job
	Type xasync.InvocationType
}
type invocationKey struct{}

func (i Invocation) Context(ctx context.Context) context.Context {
	return context.WithValue(ctx, invocationKey{}, i)
}
func CurrentInvocation(ctx context.Context) (Invocation, bool) {
	value, ok := ctx.Value(invocationKey{}).(Invocation)
	return value, ok
}
