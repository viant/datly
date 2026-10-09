package engine

import (
	"context"
	"errors"
	"fmt"

	"github.com/viant/datly/internal/drainowner"
)

// protectedLifetime is canonical root enrollment, including enrollment by a
// child during output composition. Ordinary completion timing stays separate.
func (s *dataScope) protectedLifetime() bool {
	if s == nil {
		return false
	}
	root := s
	if root.root != nil {
		root = root.root
	}
	root.mu.Lock()
	defer root.mu.Unlock()
	return root.bufferedScopeEnrolled || root.guardedExecution || drainowner.ActivitiesEnrolled(root.nativeInvocation)
}

// beginProtectedCompletion closes admission before joins and freezes one unit
// set for both finalization preparation and completion. It never retires the
// caller's activity or waits for an active handler. It grants no SQL drain.
func (s *dataScope) beginProtectedCompletion(ctx context.Context) ([]*dataScope, error) {
	root := s
	if root.root != nil {
		root = root.root
	}
	root.protectedCompletionOnce.Do(func() {
		root.mu.Lock()
		var failure error
		if root.nativeInvocation != nil {
			failure = drainowner.CloseActivities(root.nativeInvocation)
		}
		root.completionStarted = true
		for _, frame := range root.finalizers {
			if !frame.finished {
				failure = errors.Join(failure, fmt.Errorf("component %s has not finished before root completion", frame.route))
			}
		}
		root.mu.Unlock()
		// Cancel and join the native prefix after closing admission, outside locks.
		if root.nativeInvocation != nil {
			failure = appendCompletionFailure(failure, drainowner.JoinPrefixCompletion(root.nativeInvocation))
		}
		// No native callback, guard, or wait runs under root/ledger locks.
		root.resolving.Wait()
		root.registering.Wait()
		root.mu.Lock()
		units := append([]*dataScope{root}, root.units...)
		guardedFailure := root.guardedFailure
		root.mu.Unlock()
		for _, unit := range units {
			unit.once.Do(func() {})
		}
		if guardedFailure != nil {
			failure = appendCompletionFailure(failure, guardedFailure)
		}
		if guardErr := root.writeEligibility.capturedExecutionFailure(); guardErr != nil {
			failure = appendCompletionFailure(failure, guardErr)
		}
		root.writeEligibility.closeCompletion(true)
		for _, unit := range units {
			if unit.err != nil {
				failure = errors.Join(failure, unit.err)
				if !unit.nativeCleanup {
					// The startup failed before genuine native binding. Closure
					// must not borrow another invocation's lifecycle authority.
					continue
				}
			}
			if unit.data == nil {
				continue
			}
			closer, ok := unit.data.(interface{ CloseMutationAdmission() error })
			if !ok {
				failure = errors.Join(failure, fmt.Errorf("protected invocation requires mutation admission closure"))
				continue
			}
			if closeErr := completionOperation("close unit mutation admission", closer.CloseMutationAdmission); closeErr != nil {
				failure = errors.Join(failure, closeErr)
			}
		}
		if root.nativeInvocation != nil && (root.bufferedScopeEnrolled || root.nativeInvocation.HasPrefixExecution()) {
			ordered, err := root.nativeInvocation.FreezeJournal()
			root.orderedCompletion = ordered
			failure = errors.Join(failure, err)
		}
		root.protectedCompletionUnits = units
		root.protectedCompletionErr = failure
	})
	return root.protectedCompletionUnits, root.protectedCompletionErr
}

// prepareProtectedFinalization uses the same closed unit set as completion.
// This sequencing seam is not sealed native drain authorization; the native
// permit implementation is a separate, still-required foundation.
func (s *dataScope) prepareProtectedFinalization(ctx context.Context) error {
	root := s
	if root.root != nil {
		root = root.root
	}
	units, err := root.beginProtectedCompletion(ctx)
	if err != nil {
		return err
	}
	if err = s.protectedCompletionFailure(nil); err != nil {
		return err
	}
	if err = validateUnitExecutionGuards(ctx, units); err != nil {
		return err
	}
	if root.orderedCompletion {
		return root.prepareOrderedJournal(ctx, units, drainowner.LocalPreparation)
	}
	for _, unit := range units {
		if preparer, ok := unit.data.(invocationFinalizationPreparer); ok {
			if err = completionOperation("data finalization preparation", func() error {
				if unit.nativeCleanup {
					return unit.callNativeDrain(ctx, drainowner.LocalPreparation, nil)
				}
				return preparer.PrepareFinalization(ctx)
			}); err != nil {
				return err
			}
		}
	}
	return nil
}

// protectedCompletionFailure is fresh evidence. Cached closure never replaces
// failures published by a later callback; ordinary pre-enrollment violations
// remain outside the protected failure channel.
func (s *dataScope) protectedCompletionFailure(existing error) error {
	root := s
	if root.root != nil {
		root = root.root
	}
	root.mu.Lock()
	issuer := root.nativeInvocation
	guardedFailure := root.guardedFailure
	root.mu.Unlock()
	failure := existing
	if issuer != nil {
		failure = appendCompletionFailure(failure, drainowner.CheckActivities(issuer))
	}
	if guardedFailure != nil {
		failure = appendCompletionFailure(failure, guardedFailure)
	}
	if guardErr := root.writeEligibility.capturedExecutionFailure(); guardErr != nil {
		failure = appendCompletionFailure(failure, guardErr)
	}
	return failure
}

// Join branches are deduplicated by their actual error identity/Is contract.
// Single-cause public wrappers are deliberately retained, not peeled away.
func appendCompletionFailure(existing, supplemental error) error {
	if supplemental == nil || (existing != nil && errors.Is(existing, supplemental)) {
		return existing
	}
	if joined, ok := supplemental.(interface{ Unwrap() []error }); ok {
		result := existing
		for _, branch := range joined.Unwrap() {
			result = appendCompletionFailure(result, branch)
		}
		return result
	}
	if existing == nil {
		return supplemental
	}
	return errors.Join(existing, supplemental)
}

// rejectProtected records failed terminal admission on the canonical root.
func (s *dataScope) rejectProtected(err error) error {
	if s == nil {
		return err
	}
	root := s
	if root.root != nil {
		root = root.root
	}
	root.mu.Lock()
	defer root.mu.Unlock()
	return root.rejectProtectedLocked(err)
}
func (s *dataScope) rejectProtectedLocked(err error) error {
	if s.bufferedScopeEnrolled || s.guardedExecution || drainowner.ActivitiesEnrolled(s.nativeInvocation) {
		s.guardedFailure = errors.Join(s.guardedFailure, err)
	}
	return err
}

// Read each failure channel independently; never call user guards or acquire
// root/native locks while holding the proxy guard lock.
func (s *dataScope) protectedAdmissionFailure() error {
	if s == nil {
		return nil
	}
	root := s
	if root.root != nil {
		root = root.root
	}
	root.mu.Lock()
	issuer := root.nativeInvocation
	protected := root.bufferedScopeEnrolled || root.guardedExecution || drainowner.ActivitiesEnrolled(issuer)
	failure := root.guardedFailure
	root.mu.Unlock()
	if !protected {
		return nil
	}
	if failure != nil {
		return failure
	}
	if failure = drainowner.ProtectedFailure(issuer); failure != nil {
		return failure
	}
	return root.writeEligibility.capturedExecutionFailure()
}
