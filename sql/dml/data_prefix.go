package dml

import (
	"context"
	"errors"

	"github.com/viant/datly/internal/drainowner"
)

// preparePrefix is dispatched only by the owner's constructor-bound operation.
// Its permit executes a selected database prefix, never a commit.
func (d *Data) preparePrefix(ctx context.Context, permit *drainowner.DrainPermit) error {
	owner := d.owner()
	owner.executionMu.Lock()
	defer owner.executionMu.Unlock()
	record, err := drainowner.ConsumeDrain(owner, permit, drainowner.PrefixPreparation, nil)
	if err != nil {
		return err
	}
	defer drainowner.EndDrain(record)
	targetValue, frame, table, err := drainowner.PrefixTarget(owner, permit)
	if err != nil {
		return err
	}
	target, ok := targetValue.(*Data)
	if !ok || target.owner() != owner {
		return drainowner.ErrDrainPermit
	}
	owner.mu.Lock()
	switch {
	case !owner.invocation || owner.externalTx:
		err = drainowner.ErrOrderedComposition
	case owner.mutationAdmissionClosed || owner.completed || !target.open || target.journalFrame != frame:
		err = ErrComponentSealed
	case owner.failed != nil:
		err = errors.Join(ErrInvocationFailed, owner.failed)
	}
	owner.mu.Unlock()
	if err != nil {
		return err
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	if err := owner.validateExecutionGuardsLocked(ctx); err != nil {
		return err
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	matched := owner.operations(table, target)
	records := make([]drainowner.JournalRecord, len(matched))
	for i, operation := range matched {
		records[i] = drainowner.JournalRecord{Record: operation, Frame: operation.journalFrame, ID: operation.id}
	}
	if err := drainowner.BindPrefixRecords(owner, permit, records); err != nil {
		return err
	}
	return owner.executePendingLocked(ctx, matched, permit)
}
