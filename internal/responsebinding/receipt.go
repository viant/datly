// Package responsebinding owns one-call declared response projection authority.
// Applications and transports cannot construct or attach a projection token.
package responsebinding

import (
	"context"
	"errors"
	dexec "github.com/viant/datly/exec"
	legacy "github.com/viant/xdatly/handler/response"
	"reflect"
	"sync"
)

type Key struct{}
type ChildKey struct{}
type Projector func(*legacy.Error) (any, bool, error)
type Token struct {
	mu                           sync.Mutex
	owner                        any
	route                        string
	output                       reflect.Type
	projector                    Projector
	claimed, attempted, consumed bool
	response                     *legacy.Error
}

func New(ctx context.Context, owner any, route string, output reflect.Type, projector Projector) (context.Context, *Token) {
	r := &Token{owner: owner, route: route, output: output, projector: projector}
	return context.WithValue(ctx, Key{}, r), r
}
func Claim(ctx context.Context, owner any, route string, output reflect.Type) (context.Context, *Token) {
	r, _ := ctx.Value(Key{}).(*Token)
	ctx = context.WithValue(ctx, Key{}, (*Token)(nil))
	if r == nil {
		return ctx, nil
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	// Absent ownership matches only absent ownership; nil is never a wildcard.
	if r.claimed || r.owner != owner || r.route != route || r.output != output {
		return ctx, nil
	}
	r.claimed = true
	return ctx, r
}

// Project classifies the direct own-finalizer result before activity completion.
// It never clears or mutates an engine, writer, or shared-owner failure.
func (r *Token) Project(owner any, raw, current error, calls int) (any, bool, error) {
	if r == nil {
		return nil, false, nil
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if !r.claimed || r.attempted || r.owner != owner {
		return nil, false, nil
	}
	r.attempted = true
	e, ok := raw.(*legacy.Error)
	if !ok || e == nil || current != e || calls != 1 || e.Code < 400 || e.Code > 499 || e.Message == "" || e.Err != nil || e.Object != nil || e.View != "" || e.Parameter != "" || r.projector == nil {
		return nil, false, nil
	}
	value, applicable, err := project(r.projector, e)
	if err != nil {
		return nil, false, errors.Join(e, err)
	}
	if !applicable {
		return nil, false, nil
	}
	r.response = e
	return value, true, nil
}

// Consume is called only after ordinary execution and completion succeeded.
func (r *Token) Consume() *legacy.Error {
	if r == nil {
		return nil
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.consumed {
		return nil
	}
	r.consumed = true
	return r.response
}

func project(projector Projector, candidate *legacy.Error) (value any, applicable bool, err error) {
	defer func() {
		if value := recover(); value != nil {
			err = dexec.NewPanicError("declared response projection", value)
		}
	}()
	return projector(candidate)
}
