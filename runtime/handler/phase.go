package handler

import (
	"context"
	"errors"
	dexec "github.com/viant/datly/exec"
	h "github.com/viant/xdatly/handler"
	xlogger "github.com/viant/xdatly/logger"
	"sync/atomic"
)

// PhaseObserverFactory creates an invocation-local observer without binding
// input or resolving dependent views. The engine supplies the trusted logger
// in the observation context before invoking callbacks.
type PhaseObserverFactory interface{ NewPhaseObserver() h.PhaseObserver }

// ValidationPhaseFailure marks an ordinary collected validation result. A
// validator's operational error may wrap Validation too, so type alone is not
// sufficient to classify the phase as a set of ordinary violations.
func ValidationPhaseFailure(cause error) error {
	if cause == nil {
		return nil
	}
	return &phaseValidationFailure{cause}
}

type phaseValidationFailure struct{ error }

func (e *phaseValidationFailure) Unwrap() error { return e.error }

type phaseContextKey struct{}

var phaseInvocationCounter atomic.Uint64

// PhaseScope is used synchronously by one invocation attempt.
type PhaseScope struct {
	observer      h.PhaseObserver
	id            uint64
	attempt       int
	reportedPanic bool
}

func WithPhaseObserver(ctx context.Context, observer h.PhaseObserver, id uint64, attempt int) (context.Context, *PhaseScope) {
	if observer == nil {
		if PhaseScopeFromContext(ctx) != nil {
			ctx = context.WithValue(ctx, phaseContextKey{}, (*PhaseScope)(nil))
		}
		return ctx, nil
	}
	if id == 0 {
		id = phaseInvocationCounter.Add(1)
	}
	scope := &PhaseScope{observer: observer, id: id, attempt: attempt}
	return context.WithValue(ctx, phaseContextKey{}, scope), scope
}
func PhaseScopeFromContext(ctx context.Context) *PhaseScope {
	scope, _ := ctx.Value(phaseContextKey{}).(*PhaseScope)
	return scope
}
func (s *PhaseScope) Observer() h.PhaseObserver {
	if s == nil {
		return nil
	}
	return s.observer
}
func (s *PhaseScope) InvocationID() uint64 {
	if s == nil {
		return 0
	}
	return s.id
}
func (s *PhaseScope) Notify(ctx context.Context, phase h.InvocationPhase, boundary h.PhaseBoundary, cause error) {
	if s == nil {
		return
	}
	event := h.PhaseEvent{Phase: phase, Boundary: boundary, InvocationID: s.id, Attempt: s.attempt, Cause: cause}
	if boundary == h.PhaseEnd {
		ctx = context.WithoutCancel(ctx)
		event.Result = h.PhaseSucceeded
		var panicError *dexec.PanicError
		var validation *phaseValidationFailure
		switch {
		case errors.As(cause, &panicError):
			event.Result = h.PhasePanicked
		case errors.Is(cause, context.Canceled) || errors.Is(cause, context.DeadlineExceeded):
			event.Result = h.PhaseCanceled
		case errors.As(cause, &validation):
			event.Result = h.PhaseViolations
		case cause != nil:
			event.Result = h.PhaseFailed
		}
	}
	defer func() {
		if recover() != nil && !s.reportedPanic {
			s.reportedPanic = true
			// Diagnostic reporting must not let a second logger panic alter execution.
			func() {
				defer func() { _ = recover() }()
				if logger := xlogger.FromContext(ctx); logger != nil {
					logger.Error("phase observer panicked")
				}
			}()
		}
	}()
	s.observer.ObservePhase(ctx, event)
}

// Run pairs actual phase entry/exit, preserving errors and panics for the
// existing invocation owner. Skipped phases must not call Run.
func (s *PhaseScope) Run(ctx context.Context, phase h.InvocationPhase, fn func() error) (err error) {
	if s == nil {
		return fn()
	}
	s.Notify(ctx, phase, h.PhaseBegin, nil)
	defer func() {
		if value := recover(); value != nil {
			s.Notify(ctx, phase, h.PhaseEnd, dexec.NewPanicError("invocation phase", value))
			panic(value)
		}
		s.Notify(ctx, phase, h.PhaseEnd, err)
	}()
	return fn()
}
