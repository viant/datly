package engine

import (
	"context"
	"errors"
	"fmt"
	"sync"

	dexec "github.com/viant/datly/exec"
	xhandler "github.com/viant/xdatly/handler"
)

// finalizerInjectors limits route capabilities to the pre-completion callback.
// Serial admission also makes close wait for in-flight child work before the
// engine can seal the component or complete its root transaction.
type finalizerInjectors struct {
	data   *dataScope
	mu     sync.Mutex
	ctx    context.Context
	output any
	lookup func(context.Context, any, xhandler.Route) (xhandler.Binder, error)
	closed bool
	err    error
}

func (s *finalizerInjectors) finalize(finalizer xhandler.InjectorFinalizer, cause error) (err error) {
	defer func() {
		if value := recover(); value != nil {
			err = dexec.NewPanicError("injector finalizer", value)
		}
		s.mu.Lock()
		defer s.mu.Unlock()
		s.closed = true
		s.record(err)
		err = cause
		if s.err != nil && !errors.Is(cause, s.err) {
			if err == nil {
				err = s.err
			} else {
				err = errors.Join(err, s.err)
			}
		}
	}()
	return finalizer.Finalize(s.ctx, s.get)
}

func (s *finalizerInjectors) run(call context.Context, fn func(context.Context) error) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return fmt.Errorf("injector finalizer scope is closed")
	}
	if call == nil {
		s.record(fmt.Errorf("injector call context is required"))
		return s.err
	}
	ctx, cancel := context.WithCancel(s.ctx)
	stop := context.AfterFunc(call, cancel)
	defer stop()
	s.data.retainContext(cancel)
	if err := call.Err(); err != nil {
		cancel()
	}
	err := ctx.Err()
	if err == nil {
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
	s.record(err)
	return err
}

func (s *finalizerInjectors) get(ctx context.Context, route xhandler.Route) (xhandler.Binder, error) {
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
	return &finalizerBinder{scope: s, delegate: binder}, nil
}

type finalizerBinder struct {
	scope    *finalizerInjectors
	delegate xhandler.Binder
}

func (b *finalizerBinder) Bind(ctx context.Context, target any) error {
	return b.scope.run(ctx, func(ctx context.Context) error { return b.delegate.Bind(ctx, target) })
}
func (b *finalizerBinder) Lookup(ctx context.Context, key xhandler.ValueKey) (value any, found bool, err error) {
	err = b.scope.run(ctx, func(ctx context.Context) error {
		var err error
		value, found, err = b.delegate.Lookup(ctx, key)
		return err
	})
	return
}

func (s *finalizerInjectors) record(err error) {
	if err == nil || errors.Is(s.err, err) {
		return
	}
	if s.err == nil {
		s.err = err
	} else {
		s.err = errors.Join(s.err, err)
	}
}
