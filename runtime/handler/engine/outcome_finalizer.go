package engine

import (
	"context"
	"errors"
	"fmt"

	rhandler "github.com/viant/datly/runtime/handler"
	xhandler "github.com/viant/xdatly/handler"
	"github.com/viant/xdatly/response"
)

type outcomeFrame struct {
	ctx        context.Context
	route      string
	finalizer  rhandler.OutcomeFinalizer
	invocation rhandler.Invocation
	result     any
	err        error
	finished   bool
}

// FinalizationError distinguishes post-completion work from a failed database
// transaction. Outcome retains the actual commit/rollback evidence.
type FinalizationError struct {
	Route   string
	Outcome xhandler.Outcome
	Err     error
}

func (e *FinalizationError) Error() string {
	return fmt.Sprintf("finalize %s after %s: %v", e.Route, e.Outcome.State(), e.Err)
}
func (e *FinalizationError) Unwrap() error { return e.Err }

func neutralDataScope() *dataScope {
	scope := &dataScope{bySource: map[any]*dataScope{}}
	scope.root, scope.unit = scope, scope
	return scope
}

func (s *dataScope) registerOutcome(ctx context.Context, route string, finalizer rhandler.OutcomeFinalizer) (*outcomeFrame, error) {
	root := s.root
	if root == nil {
		root = s
	}
	root.mu.Lock()
	defer root.mu.Unlock()
	if root.completionStarted || root.finalized {
		return nil, fmt.Errorf("invocation completion has already started")
	}
	frame := &outcomeFrame{ctx: ctx, route: route, finalizer: finalizer}
	root.finalizers = append(root.finalizers, frame)
	return frame, nil
}

func (s *dataScope) finishOutcome(frame *outcomeFrame, invocation rhandler.Invocation, result any, err error) {
	if frame == nil {
		return
	}
	root := s.root
	if root == nil {
		root = s
	}
	root.mu.Lock()
	defer root.mu.Unlock()
	frame.invocation, frame.result, frame.err, frame.finished = invocation, result, err, true
}

func (s *dataScope) finalizeOutcomes(completionErr error) error {
	if s == nil {
		return completionErr
	}
	root := s.root
	if root == nil {
		root = s
	}
	root.mu.Lock()
	if root.finalized {
		root.mu.Unlock()
		return completionErr
	}
	root.finalized = true
	frames := make([]outcomeFrame, len(root.finalizers))
	for index, frame := range root.finalizers {
		frames[index] = *frame
	}
	outcome := root.completion.Clone()
	root.mu.Unlock()
	result := outcomeErrors{err: completionErr}
	// Child callbacks finish before their parent's callback, but never before
	// the shared root's transaction completion. Nested calls must be awaited.
	for index := len(frames) - 1; index >= 0; index-- {
		frame := frames[index]
		if !frame.finished {
			result.add(&FinalizationError{Route: frame.route, Outcome: outcome.Clone(), Err: fmt.Errorf("component has not finished before root finalization")})
			continue
		}
		observed := outcome.Clone()
		if frame.err != nil && !errors.Is(observed.Error, frame.err) {
			observed.Error = errors.Join(observed.Error, frame.err)
		}
		if err := frame.finalizer.FinalizeOutcome(frame.ctx, frame.invocation, frame.result, observed.Clone()); err != nil {
			result.add(&FinalizationError{Route: frame.route, Outcome: observed.Clone(), Err: err})
		}
	}
	return result.Err()
}

// outcomeErrors keeps completion and callback diagnostics in their original
// order. Only a newly supplied public body changes protocol projection.
type outcomeErrors struct {
	err    error
	public response.BodyError
}

func (e *outcomeErrors) add(err *FinalizationError) {
	e.err = errors.Join(e.err, err)
	if public := e.publicError(err.Err, err.Outcome.Error); public != nil {
		// Callbacks run child-first, so an explicit parent policy wins. A nil
		// callback or ordinary cleanup error leaves the child's policy intact.
		e.public = public
	}
}

func (e *outcomeErrors) publicError(err, original error) response.BodyError {
	// Wrapping or joining an existing operation error is cleanup. A fresh
	// BodyError, including one wrapping that operation, deliberately maps it.
	if err == nil || errors.Is(original, err) {
		return nil
	}
	if public, ok := err.(response.BodyError); ok {
		return public
	}
	switch wrapped := err.(type) {
	case interface{ Unwrap() error }:
		return e.publicError(wrapped.Unwrap(), original)
	case interface{ Unwrap() []error }:
		for _, child := range wrapped.Unwrap() {
			if public := e.publicError(child, original); public != nil {
				return public
			}
		}
	}
	return nil
}

func (e *outcomeErrors) Err() error {
	if e.public == nil {
		return e.err
	}
	return &publicOutcomeError{BodyError: e.public, cause: e.err}
}

// Public projection is independent of the retained control/diagnostic chain.
// In particular, a mapped failure never rewrites transaction outcome evidence.
type publicOutcomeError struct {
	response.BodyError
	cause error
}

func (e *publicOutcomeError) Error() string { return e.cause.Error() }
func (e *publicOutcomeError) Unwrap() error { return e.cause }
