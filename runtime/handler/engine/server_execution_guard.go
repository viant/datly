package engine

import (
	"context"
	"fmt"
)

// A neutral orchestration scope has no transaction owner. Retain trusted checks
// there until an actual data capability resolves, before exposing it to code.
func (s *dataScope) deferServerExecutionChecks(checks []func(context.Context) error) {
	root := s.root
	if root == nil {
		root = s
	}
	root.mu.Lock()
	root.serverExecutionChecks = append(root.serverExecutionChecks, checks...)
	root.mu.Unlock()
}

func (s *dataScope) enrollServerExecutionChecks(ctx context.Context) error {
	root := s.root
	if root == nil {
		root = s
	}
	unit := s.unit
	if unit == nil {
		unit = s
	}
	root.mu.Lock()
	if unit.data == nil {
		root.mu.Unlock()
		return nil
	}
	start := unit.serverExecutionEnrolled
	checks := append([]func(context.Context) error(nil), root.serverExecutionChecks[start:]...)
	// Advance before registration, which may resolve this same existing unit.
	unit.serverExecutionEnrolled = len(root.serverExecutionChecks)
	root.mu.Unlock()
	if len(checks) == 0 {
		return nil
	}
	owner, ok := unit.data.(guardedInvocationData)
	if !ok || !unit.nativeCleanup {
		return root.failGuardedExecution(fmt.Errorf("server execution guard requires the retained native invocation owner"))
	}
	// This is an authorization fence on the existing owner, not captured-writer
	// prefix/activity enrollment. Already protected scopes keep their strict
	// public-drain denial; ordinary imperative preparation stays owner-bound.
	for _, check := range checks {
		if err := completionOperation("register server execution owner guard", func() error { return owner.RegisterExecutionGuard(check) }); err != nil {
			return root.failGuardedExecution(err)
		}
	}
	return nil
}
