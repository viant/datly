package handler

import (
	"context"
	"fmt"
)

type serverExecutionGuardKey struct{}

// WithServerExecutionGuard installs an embedding server's side-effect-free
// invocation check. It is context-only: never decoded from component inputs,
// request metadata, schemas or an MCP wire extension. The engine captures it
// before input initialization and retains it on the existing mutation owner.
// Nil preserves the ordinary invocation behavior.
func WithServerExecutionGuard(ctx context.Context, check func(context.Context) error) context.Context {
	if check == nil {
		return ctx
	}
	guards := ServerExecutionGuards(ctx)
	guards = append(guards, check)
	return context.WithValue(ctx, serverExecutionGuardKey{}, guards)
}

// ServerExecutionGuards returns a detached list for engine capture. It carries
// no identity, authorization dependency, transaction or completion ownership.
func ServerExecutionGuards(ctx context.Context) []func(context.Context) error {
	guards, _ := ctx.Value(serverExecutionGuardKey{}).([]func(context.Context) error)
	return append([]func(context.Context) error(nil), guards...)
}

// CheckServerExecutionGuards fences completion in specialized adapters which
// retain their own existing transaction. It never starts, commits or rolls back
// a unit; the adapter remains its sole completion owner.
func CheckServerExecutionGuards(ctx context.Context) (err error) {
	defer func() {
		if recover() != nil {
			err = fmt.Errorf("server execution guard failed")
		}
	}()
	for _, check := range ServerExecutionGuards(ctx) {
		if err = ctx.Err(); err != nil {
			return err
		}
		if err = check(ctx); err != nil {
			return err
		}
	}
	return nil
}

// CaptureServerExecutionGuards transfers only installed server checks to an
// explicitly owned continuation and caps it to the original dispatch deadline.
// It does not copy credentials, claims, component input or arbitrary metadata.
// An ordinary continuation with no installed checks keeps its existing context.
func CaptureServerExecutionGuards(base, source context.Context) (context.Context, context.CancelFunc) {
	checks := ServerExecutionGuards(source)
	if len(checks) == 0 {
		return base, func() {}
	}
	for _, check := range checks {
		base = WithServerExecutionGuard(base, check)
	}
	if deadline, ok := source.Deadline(); ok {
		return context.WithDeadline(base, deadline)
	}
	return base, func() {}
}
