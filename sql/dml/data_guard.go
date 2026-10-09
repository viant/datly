package dml

import (
	"context"
	"errors"
	"github.com/viant/datly/internal/drainowner"
)

// RegisterExecutionGuard is a Datly-internal optional lifecycle capability.
// Component views retain checks on the journal owner, beyond child sealing.
// The engine supplies captured-program checks, never request/output callbacks.
type registeredExecutionGuard struct {
	check   func(context.Context) error
	binding *drainowner.GuardBinding
	bound   bool
}

func (d *Data) RegisterExecutionGuard(check func(context.Context) error) error {
	return d.registerExecutionGuard(check, drainowner.NewGuardBinding(), false)
}

// RegisterBoundExecutionGuard preserves ordinary registration timing and binds
// purpose evidence to the exact callback registration on the native owner.
func (d *Data) RegisterBoundExecutionGuard(check func(context.Context) error, binding *drainowner.GuardBinding) error {
	return d.registerExecutionGuard(check, binding, true)
}

func (d *Data) registerExecutionGuard(check func(context.Context) error, binding *drainowner.GuardBinding, bound bool) error {
	if d == nil || check == nil {
		return errors.New("captured DML execution guard is required")
	}
	owner := d.owner()
	if err := owner.precheckProtectedMutation(); err != nil {
		return err
	}
	owner.executionMu.Lock()
	defer owner.executionMu.Unlock()
	owner.mu.Lock()
	defer owner.mu.Unlock()
	if failure := drainowner.ProtectedOwnerFailure(owner); failure != nil {
		return failure
	}
	if owner.mutationAdmissionClosed {
		return owner.failProtectedMutationLocked(ErrMutationAdmissionClosed)
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
	if err := drainowner.BindExecutionGuard(owner, binding); err != nil {
		return owner.failProtectedMutationLocked(err)
	}
	owner.executionGuards = append(owner.executionGuards, registeredExecutionGuard{check: check, binding: binding, bound: bound})
	return nil
}

// ValidateExecutionGuards supports the engine's all-unit pre-commit preflight.
// Native flush also checks under the same existing execution serialization.
func (d *Data) ValidateExecutionGuards(ctx context.Context) error {
	owner := d.owner()
	if err := drainowner.CheckProtectedDrainInFlight(owner); err != nil {
		return err
	}
	effect, err := drainowner.BeginNativeEffect(owner)
	if err != nil {
		return err
	}
	defer drainowner.EndDrain(effect)
	owner.executionMu.Lock()
	defer owner.executionMu.Unlock()
	return owner.validateExecutionGuardsLocked(ctx)
}

func (d *Data) validateExecutionGuardsLocked(ctx context.Context, terminalBoundary ...bool) error {
	owner := d.owner()
	owner.mu.Lock()
	checks := append([]registeredExecutionGuard(nil), owner.executionGuards...)
	terminal := owner.mutationAdmissionClosed || owner.completed
	if len(terminalBoundary) != 0 {
		terminal = terminal || terminalBoundary[0]
	}
	failed := owner.failed
	enabled := owner.guardsEnabled
	operations := flattenData(owner)
	owner.mu.Unlock()
	// An additional native terminal-boundary check must not add callback or
	// payload checks to the ordinary preparation path.
	if len(terminalBoundary) > 1 && terminalBoundary[1] {
		selected := checks[:0]
		for _, check := range checks {
			if check.bound {
				selected = append(selected, check)
			}
		}
		checks = selected
		operations = nil
	}
	if enabled && failed != nil {
		return errors.Join(ErrInvocationFailed, failed)
	}
	if len(checks) == 0 && !hasQueuePayloadEvidence(operations) {
		return nil
	}
	if failed != nil {
		return errors.Join(ErrInvocationFailed, failed)
	}
	err := ctx.Err()
	if err == nil {
		for _, check := range checks {
			err = completeOperation("captured DML execution guard", func() error {
				if !check.bound {
					return check.check(ctx)
				}
				guardCtx, close, scopeErr := drainowner.WithGuardPurpose(ctx, owner, check.binding, terminal)
				if scopeErr != nil {
					return scopeErr
				}
				defer close()
				return check.check(guardCtx)
			})
			if err != nil {
				break
			}
		}
	}
	if err == nil {
		for _, operation := range operations {
			if err = operation.validatePayload(); err != nil {
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
	if err := drainowner.CheckProtectedDrainInFlight(owner); err != nil {
		return err
	}
	owner.executionMu.Lock()
	defer owner.executionMu.Unlock()
	owner.mu.Lock()
	defer owner.mu.Unlock()
	if !owner.invocation {
		return errors.New("DML mutation admission requires an invocation owner")
	}
	owner.mutationAdmissionClosed = true
	drainowner.Seal(owner)
	return nil
}

// Caller holds executionMu; internal journal draining does not use this gate.
func (d *Data) checkMutationAdmissionLocked() error {
	owner := d.owner()
	owner.mu.Lock()
	defer owner.mu.Unlock()
	if failure := drainowner.ProtectedOwnerFailure(owner); failure != nil {
		return failure
	}
	if owner.mutationAdmissionClosed {
		return owner.failProtectedMutationLocked(ErrMutationAdmissionClosed)
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
	if err := owner.precheckProtectedMutation(); err != nil {
		return err
	}
	owner.executionMu.Lock()
	defer owner.executionMu.Unlock()
	owner.mu.Lock()
	defer owner.mu.Unlock()
	return owner.enableCapturedExecutionGuardsLocked()
}

func (d *Data) enableCapturedExecutionGuardsLocked() error {
	if d.mutationAdmissionClosed {
		return d.failProtectedMutationLocked(ErrMutationAdmissionClosed)
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
		return d.failProtectedMutationLocked(ErrGuardedStreamingQuery)
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
	if failure := drainowner.ProtectedOwnerFailure(owner); failure != nil {
		return failure
	}
	if owner.mutationAdmissionClosed {
		return owner.failProtectedMutationLocked(ErrMutationAdmissionClosed)
	}
	if owner.guardsEnabled {
		if owner.failed == nil {
			owner.failed = ErrGuardedStreamingQuery
		}
		return owner.failProtectedMutationLocked(ErrGuardedStreamingQuery)
	}
	owner.streamingQueryUsed = true
	return nil
}

// Caller holds owner.mu. Ordinary caught rejections remain non-sticky.
func (d *Data) failProtectedMutationLocked(cause error) error {
	if drainowner.FailProtectedOwner(d, cause) && d.failed == nil {
		d.failed = cause
	}
	return cause
}

// This prompt check only rejects protected work. Serialized admission remains
// authoritative; no mutex is retained while acquiring executionMu.
func (d *Data) precheckProtectedMutation() error {
	owner := d.owner()
	if err := drainowner.CheckPrefixMutation(owner); err != nil {
		return err
	}
	owner.mu.Lock()
	defer owner.mu.Unlock()
	if !drainowner.OwnerActivitiesEnrolled(owner) {
		return nil
	}
	if failure := drainowner.ProtectedOwnerFailure(owner); failure != nil {
		return failure
	}
	if owner.mutationAdmissionClosed {
		return owner.failProtectedMutationLocked(ErrMutationAdmissionClosed)
	}
	return nil
}
