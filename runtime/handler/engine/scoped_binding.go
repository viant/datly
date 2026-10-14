package engine

import (
	"context"
	"errors"
	"fmt"
	"github.com/viant/bindly"
	rhandler "github.com/viant/datly/runtime/handler"
	"github.com/viant/datly/runtime/registry"
	xhandler "github.com/viant/xdatly/handler"
	xlogger "github.com/viant/xdatly/logger"
	"reflect"
	"sync"
)

func validateScopedRequest(r Request) error {
	if r.BoundInput != nil || (r.ResolvedInput == nil && len(r.ResolvedPaths) != 0) || r.Replay != nil || r.CanonicalReader || r.DataSource != nil || r.SequenceStrategy != "" || len(r.ProtectedFlushTables) != 0 || r.Completion != nil || r.IndependentChildTransactions || r.hasCompositionFinalizer() || r.hasInvokerInitializer() {
		return fmt.Errorf("scoped input binding requires a source-less custom invocation without prepared input or completion orchestration")
	}
	return r.Input.PrepareScopePlans()
}

// scopedBinder is one invocation lease over the existing composed injector.
// It owns neither providers, data roots nor transaction completion.
type scopedBinder struct {
	ctx      context.Context
	injector *bindly.Injector
	input    any
	route    *registry.RouteInputContract
	data     *dataScope
	phases   *rhandler.PhaseScope
	mu       sync.Mutex
	idle     *sync.Cond
	active   bool
	closed   bool
	escaped  bool
}

func newScopedBinder(ctx context.Context, injector *bindly.Injector, input any, route *registry.RouteInputContract, data *dataScope, phases *rhandler.PhaseScope) *scopedBinder {
	b := &scopedBinder{ctx: ctx, injector: injector, input: input, route: route, data: data, phases: phases}
	b.idle = sync.NewCond(&b.mu)
	return b
}
func (b *scopedBinder) begin() error {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.closed {
		return fmt.Errorf("scoped input binder is closed")
	}
	if b.active {
		return fmt.Errorf("scoped input binder does not support overlapping or reentrant calls")
	}
	b.active = true
	return nil
}
func (b *scopedBinder) end() { b.mu.Lock(); b.active = false; b.idle.Broadcast(); b.mu.Unlock() }
func (b *scopedBinder) close() error {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.closed = true
	if b.active {
		b.escaped = true
	}
	for b.active {
		b.idle.Wait()
	}
	if b.escaped {
		return fmt.Errorf("scoped input binding escaped handler execution")
	}
	return nil
}
func (b *scopedBinder) run(call context.Context, fn func(context.Context) error) error {
	if err := b.begin(); err != nil {
		return err
	}
	defer b.end()
	if call == nil {
		return fmt.Errorf("scoped input binding context is required")
	}
	values := RetainMutationAuthority(b.ctx)(lifecycleCallContext{Context: b.ctx, call: call})
	values = context.WithValue(values, reflect.TypeOf(b.input), b.input)
	values = xlogger.WithContext(values, xlogger.FromContext(b.ctx))
	var ctx context.Context
	var cancel context.CancelFunc
	if deadline, ok := call.Deadline(); ok {
		ctx, cancel = context.WithDeadline(values, deadline)
	} else {
		ctx, cancel = context.WithCancel(values)
	}
	stop := context.AfterFunc(call, cancel)
	defer stop()
	if b.data != nil {
		b.data.retainContext(cancel)
	} else {
		defer cancel()
	}
	if err := call.Err(); err != nil {
		cancel()
		return err
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	err := fn(ctx)
	if canceled := call.Err(); canceled != nil && !errors.Is(err, canceled) {
		err = errors.Join(err, canceled)
	}
	if canceled := ctx.Err(); canceled != nil && !errors.Is(err, canceled) {
		err = errors.Join(err, canceled)
	}
	return err
}
func (b *scopedBinder) Bind(ctx context.Context, target any) error {
	return b.BindScope(ctx, target, "")
}
func (b *scopedBinder) BindScope(ctx context.Context, target any, scope string) error {
	// The canonical dynamic type is always a nonnil pointer; a foreign dynamic
	// type compares unequal without inspecting its value.
	if target != b.input {
		return fmt.Errorf("scoped binding target must be the canonical input pointer")
	}
	plan, ok := b.route.ScopePlan(scope)
	if !ok {
		return fmt.Errorf("input scope %q is not declared", scope)
	}
	return b.run(ctx, func(ctx context.Context) error {
		return b.phases.Run(ctx, xhandler.PhaseBinding, func() error { return b.injector.Bind(ctx, b.input, bindly.WithPlan(plan), bindly.WithSource(b.input)) })
	})
}
func (b *scopedBinder) Lookup(ctx context.Context, key xhandler.ValueKey) (value any, found bool, err error) {
	err = b.run(ctx, func(ctx context.Context) error {
		var err error
		value, found, err = rhandler.NewBinder(b.injector, b.input).Lookup(ctx, key)
		return err
	})
	return
}

var _ xhandler.ScopedBinder = (*scopedBinder)(nil)
