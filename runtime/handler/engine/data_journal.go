package engine

import (
	"context"
	"errors"
	"fmt"
	"github.com/viant/datly/internal/drainowner"
)

// Called with the root map lock. Frames are created at invocation admission,
// before source resolution, so empty and neutral components retain topology.
func (s *dataScope) ensureJournalFrameLocked(root *dataScope) {
	issuer := root.nativeIssuerLocked()
	if root.journalFrame == nil {
		root.journalFrame = issuer.RootFrame()
	}
	if s.journalFrame != nil {
		return
	}
	if s.parent == nil {
		s.journalFrame = root.journalFrame
		return
	}
	s.parent.ensureJournalFrameLocked(root)
	f, err := issuer.ChildFrame(s.parent.journalFrame, string(s.relation), s.order)
	if err != nil {
		s.err = err
		root.guardedFailure = errors.Join(root.guardedFailure, err)
		return
	}
	s.journalFrame = f
}
func (s *dataScope) orderedAdmissionLocked() error {
	if !s.bufferedScopeEnrolled {
		return nil
	}
	units := append([]*dataScope{s}, s.units...)
	count := 0
	for _, unit := range units {
		if unit.source != nil || unit.data != nil {
			count++
		}
	}
	if count < 2 {
		return nil
	}
	for _, unit := range units {
		if unit.source == nil && unit.data == nil {
			continue
		}
		if sourceTransactionKey(unit.source) != nil || (unit.data != nil && !unit.nativeCleanup) {
			s.guardedFailure = errors.Join(s.guardedFailure, drainowner.ErrOrderedComposition)
			drainowner.FailProtected(s.nativeInvocation, drainowner.ErrOrderedComposition)
			return drainowner.ErrOrderedComposition
		}
	}
	return nil
}
func (s *dataScope) prepareOrderedJournal(ctx context.Context, units []*dataScope, phase drainowner.Operation) error {
	root := s
	if root.root != nil {
		root = root.root
	}
	for {
		if err := ctx.Err(); err != nil {
			return root.failOrderedJournal(err)
		}
		if err := root.protectedCompletionFailure(nil); err != nil {
			return err
		}
		if err := validateUnitExecutionGuards(ctx, units); err != nil {
			return root.failOrderedJournal(err)
		}
		receiver := root.nativeInvocation.NextJournalOwner()
		if receiver == nil {
			return nil
		}
		var current *dataScope
		for _, unit := range units {
			if unit.data == receiver {
				current = unit
				break
			}
		}
		if current == nil {
			return root.failOrderedJournal(fmt.Errorf("frozen journal owner is outside the completion unit set"))
		}
		if err := completionOperation("ordered native preparation", func() error { return current.callNativeDrain(ctx, phase, nil) }); err != nil {
			return root.failOrderedJournal(err)
		}
	}
}
func (s *dataScope) failOrderedJournal(err error) error {
	s.failGuardedExecution(err)
	drainowner.FailProtected(s.nativeInvocation, err)
	return err
}

// Transaction creation is recorded independently of flattened statement order.
// Missing transactions are retired without inventing an ordinal.
func (s *dataScope) transactionCompletionOrder(units []*dataScope) []*dataScope {
	if !s.orderedCompletion || s.nativeInvocation == nil {
		return units
	}
	result := make([]*dataScope, 0, len(units))
	seen := map[*dataScope]bool{}
	for _, receiver := range s.nativeInvocation.TransactionOwners() {
		for _, unit := range units {
			if unit.data == receiver {
				result = append(result, unit)
				seen[unit] = true
				break
			}
		}
	}
	for _, unit := range units {
		if !seen[unit] {
			result = append(result, unit)
		}
	}
	return result
}
