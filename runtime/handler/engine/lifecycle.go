package engine

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"sync"

	dexec "github.com/viant/datly/exec"
	"github.com/viant/datly/internal/responsebinding"
	xbind "github.com/viant/xdatly/bind"
	xhandler "github.com/viant/xdatly/handler"
)

// lifecycleScope limits composition capabilities to one lifecycle callback.
// Serial admission also makes close wait for in-flight child work before the
// engine can seal the component or complete its root transaction.
type lifecycleScope struct {
	data   *dataScope
	binder xhandler.Binder
	phase  string
	mu     sync.Mutex
	ctx    context.Context
	output any
	lookup func(context.Context, any, xhandler.Route) (xhandler.Binder, error)
	closed bool
	err    error
}

func (s *lifecycleScope) finish(hook func(context.Context) error, cause error) (err error) {
	defer func() {
		if value := recover(); value != nil {
			err = dexec.NewPanicError(s.phase, value)
		}
		s.mu.Lock()
		defer s.mu.Unlock()
		s.closed = true
		s.record(err)
		// A callback can select its public response while the child's recorded
		// failure remains a veto in the retained cause chain.
		projection := (&outcomeErrors{}).publicError(err, cause)
		err = cause
		if s.err != nil && !errors.Is(cause, s.err) {
			if err == nil {
				err = s.err
			} else {
				err = errors.Join(err, s.err)
			}
		}
		if projection != nil {
			err = &publicOutcomeError{BodyError: projection, cause: err}
		}
	}()
	return hook(s.ctx)
}

func (s *lifecycleScope) run(call context.Context, fn func(context.Context) error) error {
	return s.runInvocation(call, fn, nil)
}

func (s *lifecycleScope) runInvocation(call context.Context, fn func(context.Context) error, readerCall *readerInvocation) error {
	return s.runInvocationCommit(call, fn, readerCall, nil)
}
func (s *lifecycleScope) runInvocationCommit(call context.Context, fn func(context.Context) error, readerCall *readerInvocation, commit func() error) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return fmt.Errorf("%s scope is closed", s.phase)
	}
	if call == nil {
		s.record(fmt.Errorf("%s call context is required", s.phase))
		return s.err
	}
	// Preserve per-call business markers so child and parent conversions do not
	// both run. Lease cancellation and trusted ownership remain authoritative.
	values := RetainMutationAuthority(s.ctx)(lifecycleCallContext{Context: s.ctx, call: call})
	var ctx context.Context
	var cancel context.CancelFunc
	if deadline, ok := call.Deadline(); ok {
		ctx, cancel = context.WithDeadline(values, deadline)
	} else {
		ctx, cancel = context.WithCancel(values)
	}
	stop := context.AfterFunc(call, cancel)
	defer stop()
	s.data.retainContext(cancel)
	if err := call.Err(); err != nil {
		cancel()
	}
	err := ctx.Err()
	if err == nil {
		if readerCall != nil {
			ctx = context.WithValue(ctx, readerInvocationKey{}, readerCall)
		}
		err = fn(ctx)
	}
	if canceled := call.Err(); canceled != nil {
		if err == nil {
			err = canceled
		} else if !errors.Is(err, canceled) {
			err = errors.Join(err, canceled)
		}
	}
	if err == nil {
		err = ctx.Err()
	}
	if readerCall != nil {
		if canceled := ctx.Err(); canceled != nil && !errors.Is(err, canceled) {
			err = errors.Join(err, canceled)
		}
	}
	if err == nil && commit != nil {
		err = commit()
	}
	if readerCall == nil || !readerCall.handled(err) {
		s.record(err)
	}
	return err
}

func (s *lifecycleScope) get(ctx context.Context, route xhandler.Route) (xhandler.Binder, error) {
	var binder xhandler.Binder
	err := s.run(ctx, func(ctx context.Context) error {
		if s.lookup == nil {
			return fmt.Errorf("component injector lookup is not configured")
		}
		var err error
		binder, err = s.lookup(ctx, s.output, route)
		if err == nil && binder == nil {
			return fmt.Errorf("component injector lookup returned no binder")
		}
		return err
	})
	if err != nil {
		return nil, err
	}
	return &lifecycleBinder{scope: s, delegate: binder}, nil
}

type lifecycleBinder struct {
	scope    *lifecycleScope
	delegate xhandler.Binder
}

func (b *lifecycleBinder) Bind(ctx context.Context, target any) error {
	return b.scope.run(ctx, func(ctx context.Context) error { return b.delegate.Bind(ctx, target) })
}
func (b *lifecycleBinder) Lookup(ctx context.Context, key xhandler.ValueKey) (value any, found bool, err error) {
	err = b.scope.run(ctx, func(ctx context.Context) error {
		var err error
		value, found, err = b.delegate.Lookup(ctx, key)
		return err
	})
	return
}

func (s *lifecycleScope) record(err error) {
	if err == nil || errors.Is(s.err, err) {
		return
	}
	if s.err == nil {
		s.err = err
	} else {
		s.err = errors.Join(s.err, err)
	}
}

// Invoke uses the same runtime-owned provider as field injection. Resolution is
// lazy so a hook that does not compose children needs no invoker provider.
func (s *lifecycleScope) Invoke(ctx context.Context, request xhandler.ComponentRequest) (any, error) {
	var result any
	owner := s.data
	if owner != nil && owner.root != nil {
		owner = owner.root
	}
	receipt := &readerInvocation{owner: owner}
	err := s.runInvocation(ctx, func(ctx context.Context) error {
		invoker, err := xbind.MustLookup[xhandler.ComponentInvoker](ctx, s.binder, xhandler.ComponentInvokerKey)
		if err != nil {
			return err
		}
		if invoker == nil {
			return fmt.Errorf("component invoker is not configured")
		}
		result, err = invoker.Invoke(ctx, request)
		return err
	}, receipt)
	return result, err
}

// Lifecycle calls retain lexical call-value overrides for ordinary markers.
// Private engine ownership and receipts come only from the original lease.
type lifecycleCallContext struct {
	context.Context
	call context.Context
}

func (c lifecycleCallContext) Value(key any) any {
	switch key.(type) {
	case dataScopeContextKey, componentScopeKey, readerInvocationKey, responsebinding.Key, responsebinding.ChildKey:
		return c.Context.Value(key)
	}
	if value := c.call.Value(key); value != nil {
		return value
	}
	return c.Context.Value(key)
}

// BindResponse shares the lease's serial admission, cancellation and close rules.
func (s *lifecycleScope) BindResponse(ctx context.Context, request xhandler.ComponentRequest, destination any) (bound xhandler.BoundResponse, err error) {
	owner := s.data
	if owner != nil && owner.root != nil {
		owner = owner.root
	}
	target := reflect.ValueOf(destination)
	staged := destination
	if target.IsValid() && target.Kind() == reflect.Pointer && !target.IsNil() {
		staged = reflect.New(target.Type().Elem()).Interface()
	}
	receipt := &readerInvocation{owner: owner}
	err = s.runInvocationCommit(ctx, func(ctx context.Context) error {
		invoker, lookupErr := xbind.MustLookup[xhandler.ComponentInvoker](ctx, s.binder, xhandler.ComponentInvokerKey)
		if lookupErr != nil {
			return lookupErr
		}
		binder, ok := invoker.(xhandler.ComponentResponseBinder)
		if !ok {
			return fmt.Errorf("component response binder is not configured")
		}
		var bindErr error
		bound, bindErr = binder.BindResponse(ctx, request, staged)
		return bindErr
	}, receipt, func() error {
		if failure := s.data.protectedAdmissionFailure(); failure != nil {
			return failure
		}
		if target.IsValid() && target.Kind() == reflect.Pointer && !target.IsNil() {
			target.Elem().Set(reflect.ValueOf(staged).Elem())
		}
		return nil
	})
	if err != nil {
		return xhandler.BoundResponse{}, err
	}
	return
}

var _ xhandler.ComponentResponseBinder = (*lifecycleScope)(nil)
