package dml

import (
	"context"
	"errors"
)

// RegisterExecutionGuard is a Datly-internal optional lifecycle capability.
// Component views retain checks on the journal owner, beyond child sealing.
// The engine supplies captured-program checks, never request/output callbacks.
func (d *Data) RegisterExecutionGuard(check func(context.Context) error) error {
	if d == nil || check == nil {
		return errors.New("captured DML execution guard is required")
	}
	owner := d.owner()
	owner.executionMu.Lock()
	defer owner.executionMu.Unlock()
	owner.mu.Lock()
	defer owner.mu.Unlock()
	if owner.mutationAdmissionClosed {
		return ErrMutationAdmissionClosed
	}
	if owner.completed {
		return ErrInvocationCompleted
	}
	if owner.failed != nil {
		return errors.Join(ErrInvocationFailed, owner.failed)
	}
	if !owner.invocation {
		return errors.New("captured DML execution guard requires an invocation owner")
	}
	if err := owner.enableCapturedExecutionGuardsLocked(); err != nil {
		return err
	}
	owner.executionGuards = append(owner.executionGuards, check)
	return nil
}

// ValidateExecutionGuards supports the engine's all-unit pre-commit preflight.
// Native flush also checks under the same existing execution serialization.
func (d *Data) ValidateExecutionGuards(ctx context.Context) error {
	owner := d.owner()
	owner.executionMu.Lock()
	defer owner.executionMu.Unlock()
	return owner.validateExecutionGuardsLocked(ctx)
}

func (d *Data) validateExecutionGuardsLocked(ctx context.Context) error {
	owner := d.owner()
	owner.mu.Lock()
	checks := append([]func(context.Context) error(nil), owner.executionGuards...)
	failed := owner.failed
	enabled := owner.guardsEnabled
	owner.mu.Unlock()
	if enabled && failed != nil {
		return errors.Join(ErrInvocationFailed, failed)
	}
	if len(checks) == 0 {
		return nil
	}
	if failed != nil {
		return errors.Join(ErrInvocationFailed, failed)
	}
	err := ctx.Err()
	if err == nil {
		for _, check := range checks {
			err = completeOperation("captured DML execution guard", func() error { return check(ctx) })
			if err != nil {
				break
			}
		}
	}
	if err != nil {
		owner.markFailed(err)
	}
	return err
}

// CloseMutationAdmission seals new work on this journal owner while preserving
// its admitted queue and transaction for preparation and completion.
func (d *Data) CloseMutationAdmission() error {
	if d == nil {
		return errors.New("DML mutation admission requires an owner")
	}
	owner := d.owner()
	owner.executionMu.Lock()
	defer owner.executionMu.Unlock()
	owner.mu.Lock()
	defer owner.mu.Unlock()
	if !owner.invocation {
		return errors.New("DML mutation admission requires an invocation owner")
	}
	owner.mutationAdmissionClosed = true
	return nil
}

// Caller holds executionMu; internal journal draining does not use this gate.
func (d *Data) checkMutationAdmissionLocked() error {
	owner := d.owner()
	owner.mu.Lock()
	defer owner.mu.Unlock()
	if owner.mutationAdmissionClosed {
		return ErrMutationAdmissionClosed
	}
	return nil
}

// EnableCapturedExecutionGuards enrolls the exact owner without running SQL.
// Managed streaming SQL is an explicit unsupported combination in this mode;
// neither a method return nor a consumed result establishes general lifetime.
func (d *Data) EnableCapturedExecutionGuards() error {
	if d == nil {
		return errors.New("captured DML guards require an owner")
	}
	owner := d.owner()
	owner.executionMu.Lock()
	defer owner.executionMu.Unlock()
	owner.mu.Lock()
	defer owner.mu.Unlock()
	return owner.enableCapturedExecutionGuardsLocked()
}

func (d *Data) enableCapturedExecutionGuardsLocked() error {
	if d.mutationAdmissionClosed {
		return ErrMutationAdmissionClosed
	}
	if d.completed {
		return ErrInvocationCompleted
	}
	if !d.invocation {
		return errors.New("captured DML guards require an invocation owner")
	}
	d.guardsEnabled = true
	if d.streamingQueryUsed {
		if d.failed == nil {
			d.failed = ErrGuardedStreamingQuery
		}
		return ErrGuardedStreamingQuery
	}
	if d.failed != nil {
		return errors.Join(ErrInvocationFailed, d.failed)
	}
	return nil
}

// Caller holds executionMu; record every attempted dispatch, even SQL errors
// or cancellation, so a later opted child cannot infer streaming completion.
func (d *Data) admitStreamingQueryLocked() error {
	owner := d.owner()
	owner.mu.Lock()
	defer owner.mu.Unlock()
	if owner.mutationAdmissionClosed {
		return ErrMutationAdmissionClosed
	}
	if owner.guardsEnabled {
		if owner.failed == nil {
			owner.failed = ErrGuardedStreamingQuery
		}
		return ErrGuardedStreamingQuery
	}
	owner.streamingQueryUsed = true
	return nil
}
