package dml

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	dexec "github.com/viant/datly/exec"
	"github.com/viant/datly/internal/drainowner"
	xhandler "github.com/viant/xdatly/handler"
)

func (d *Data) database() (*sql.DB, error) {
	owner := d.owner()
	if owner.db == nil {
		return nil, fmt.Errorf("DML database is required")
	}
	return owner.db, nil
}

func (d *Data) transaction(ctx context.Context) (result *sql.Tx, retErr error) {
	owner := d.owner()
	publication := drainowner.EstablishmentGate(owner)
	var established any
	defer func() { publication(established) }()
	owner.mu.Lock()
	defer func() {
		managed := owner.invocation
		owner.mu.Unlock()
		if managed && retErr != nil {
			owner.markFailed(retErr)
		}
	}()
	isolation, requested, err := requestedIsolation(ctx)
	if err != nil {
		return nil, err
	}
	if owner.tx != nil {
		if requested && (owner.externalTx || owner.txIsolation != isolation) {
			return nil, fmt.Errorf("requested transaction isolation cannot change or verify an existing transaction")
		}
		return owner.tx, nil
	}
	db, err := d.database()
	if err != nil {
		return nil, err
	}
	var options *sql.TxOptions
	if requested {
		options = &sql.TxOptions{Isolation: isolation}
	}
	tx, err := db.BeginTx(ctx, options)
	if err != nil {
		return nil, err
	}
	established = tx
	owner.tx = tx
	owner.txIsolation = isolation
	return tx, nil
}

func requestedIsolation(ctx context.Context) (sql.IsolationLevel, bool, error) {
	policy, requested := dexec.RequestedTransactionIsolation(ctx)
	if !requested {
		return sql.LevelDefault, false, nil
	}
	switch policy {
	case dexec.IsolationReadCommitted:
		return sql.LevelReadCommitted, true, nil
	case dexec.IsolationRepeatableRead:
		return sql.LevelRepeatableRead, true, nil
	case dexec.IsolationSerializable:
		return sql.LevelSerializable, true, nil
	default:
		return sql.LevelDefault, true, fmt.Errorf("unsupported transaction isolation %q", policy)
	}
}

func (d *Data) Flush(ctx context.Context, tableName string) error {
	return d.flush(ctx, tableName, d)
}

// PrepareFinalization makes local flush failures visible to an error-aware
// finalizer before commit. A supplied transaction retains its existing ordering:
// no queued writes execute until the finalizer has completed successfully.
func (d *Data) PrepareFinalization(ctx context.Context) error {
	return d.prepareNative(ctx, nil, drainowner.LocalPreparation)
}

// PrepareCompletion keeps standalone preparation public; engine-owned protected
// preparation is dispatched through its constructor-bound exact operation.
func (d *Data) PrepareCompletion(ctx context.Context) error {
	return d.prepareNative(ctx, nil, drainowner.AllPreparation)
}

func (d *Data) admitNativeDrain(permit *drainowner.DrainPermit, operation drainowner.Operation, cause error) (*drainowner.DrainRecord, error) {
	if permit == nil {
		return drainowner.BeginPublicDrain(d.owner())
	}
	return drainowner.ConsumeDrain(d.owner(), permit, operation, cause)
}

func (d *Data) prepareNative(ctx context.Context, permit *drainowner.DrainPermit, operation drainowner.Operation) error {
	owner := d.owner()
	if permit == nil {
		if err := drainowner.CheckPublicDrain(owner); err != nil {
			return err
		}
	}
	owner.executionMu.Lock()
	defer owner.executionMu.Unlock()
	record, err := owner.admitNativeDrain(permit, operation, nil)
	if err != nil {
		return err
	}
	defer drainowner.EndDrain(record)
	owner.mu.Lock()
	// Admission precedes the caller-owned local-preparation early return.
	if operation == drainowner.LocalPreparation && owner.externalTx {
		owner.mu.Unlock()
		return nil
	}
	if owner.completed {
		owner.mu.Unlock()
		return ErrInvocationCompleted
	}
	if owner.failed != nil {
		failed := owner.failed
		owner.mu.Unlock()
		return errors.Join(ErrInvocationFailed, failed)
	}
	owner.mu.Unlock()
	if permit != nil {
		records, err := drainowner.RunRecords(owner, permit)
		if err != nil {
			return err
		}
		if records != nil {
			if err := owner.validateExecutionGuardsLocked(ctx); err != nil {
				return err
			}
			ops := make([]*dataOperation, len(records))
			for i, r := range records {
				op, ok := r.(*dataOperation)
				if !ok || op.frame.owner() != owner {
					return drainowner.ErrJournal
				}
				ops[i] = op
			}
			return owner.executePendingLocked(ctx, ops)
		}
		if drainowner.OrderedJournal(owner) {
			return owner.validateExecutionGuardsLocked(ctx)
		}
	}
	return owner.flushLocked(ctx, "", nil)
}

func (d *Data) flush(ctx context.Context, tableName string, target *Data) error {
	owner := d.owner()
	if err := drainowner.CheckPublicDrain(owner); err != nil {
		return err
	}
	if target != nil {
		owner.mu.Lock()
		for frame := target; frame != nil; frame = frame.parent {
			if frame.relation != ComponentBinding {
				continue
			}
			for parent := frame.parent; parent != nil; parent = parent.parent {
				if parent.open {
					owner.mu.Unlock()
					return ErrBindingFlush
				}
			}
		}
		owner.mu.Unlock()
	}
	owner.executionMu.Lock()
	defer owner.executionMu.Unlock()
	record, err := drainowner.BeginPublicDrain(owner)
	if err != nil {
		return err
	}
	defer drainowner.EndDrain(record)
	return owner.flushLocked(ctx, tableName, target)
}

// flushLocked executes a journal prefix while the caller holds executionMu.
// Completion uses the same path so rollback/commit cannot race an explicit
// flush or transactional sequence allocation.
func (d *Data) flushLocked(ctx context.Context, tableName string, target *Data) error {
	owner := d.owner()
	if target != nil {
		owner.mu.Lock()
		if owner.completed {
			owner.mu.Unlock()
			return ErrInvocationCompleted
		}
		if owner.failed != nil {
			failed := owner.failed
			owner.mu.Unlock()
			return errors.Join(ErrInvocationFailed, failed)
		}
		owner.mu.Unlock()
	}
	if err := owner.validateExecutionGuardsLocked(ctx); err != nil {
		return err
	}
	matched := owner.operations(tableName, target)
	return owner.executePendingLocked(ctx, matched)
}

// executePendingLocked is shared by local flushes and authenticated frozen runs.
func (d *Data) executePendingLocked(ctx context.Context, matched []*dataOperation) error {
	owner := d.owner()
	if len(matched) == 0 {
		return nil
	}
	if err := ctx.Err(); err != nil {
		owner.markFailed(err)
		return err
	}
	owner.mu.Lock()
	for _, operation := range matched {
		if operation.executed || operation.reserved {
			owner.mu.Unlock()
			return fmt.Errorf("DML operation %d is already reserved or executed", operation.id)
		}
		operation.reserved = true
	}
	owner.mu.Unlock()
	defer func() {
		owner.mu.Lock()
		for _, operation := range matched {
			operation.reserved = false
		}
		owner.mu.Unlock()
	}()
	db, err := d.database()
	if err != nil {
		return err
	}
	tx, err := owner.transaction(ctx)
	if err != nil {
		return err
	}
	owner.mu.Lock()
	managedTx := !owner.externalTx && !owner.invocation
	owner.mu.Unlock()
	rollback := true
	defer func() {
		if rollback && managedTx {
			_ = tx.Rollback()
			owner.mu.Lock()
			if owner.tx == tx {
				owner.tx = nil
			}
			owner.mu.Unlock()
		}
	}()
	for _, step := range buildExecutionPlan(matched) {
		if hasQueuePayloadEvidence(step.operations) {
			if err := owner.validateExecutionGuardsLocked(ctx); err != nil {
				return err
			}
		}
		for _, operation := range step.operations {
			if err := operation.validatePayload(); err != nil {
				owner.markFailed(err)
				return err
			}
		}
		if err := owner.executePlanStep(ctx, db, tx, step); err != nil {
			owner.markFailed(err)
			return err
		}
		owner.mu.Lock()
		for _, operation := range step.operations {
			operation.executed = true
			operation.reserved = false
		}
		owner.mu.Unlock()
		drainowner.NoteJournalDrain(owner)
	}
	if managedTx {
		if err := tx.Commit(); err != nil {
			owner.markFailed(err)
			return err
		}
		owner.mu.Lock()
		if owner.tx == tx {
			owner.tx = nil
		}
		owner.mu.Unlock()
		if owner.onCommit != nil {
			owner.onCommit(ctx)
		}
	}
	rollback = false
	return nil
}

// Complete drains the invocation journal and completes only locally owned
// transactions. Caller-supplied transactions always remain open.
func (d *Data) Complete(ctx context.Context, cause error) error {
	return d.completeNative(ctx, cause, nil, drainowner.Completion)
}

func (d *Data) completeNative(ctx context.Context, cause error, permit *drainowner.DrainPermit, operation drainowner.Operation) error {
	owner := d.owner()
	if permit == nil {
		if err := drainowner.CheckPublicDrain(owner); err != nil {
			return err
		}
	}
	owner.executionMu.Lock()
	defer owner.executionMu.Unlock()
	record, admissionErr := owner.admitNativeDrain(permit, operation, cause)
	if admissionErr != nil {
		return admissionErr
	}
	defer drainowner.EndDrain(record)
	owner.SealComponent()
	owner.mu.Lock()
	if owner.completed {
		owner.mu.Unlock()
		return errors.Join(cause, ErrInvocationCompleted)
	}
	owner.completed = true
	drainowner.Retire(owner)
	failed := owner.failed
	owner.mu.Unlock()
	state := xhandler.TransactionNone
	localErr := failed
	defer func() { owner.recordOutcome(state, localErr) }()
	if failed != nil {
		cause = errors.Join(cause, failed)
	}
	if cause == nil {
		cause = completeOperation("DML completion flush", func() error {
			if drainowner.OrderedJournal(owner) {
				owner.mu.Lock()
				defer owner.mu.Unlock()
				for _, op := range flattenData(owner) {
					if !op.executed || op.reserved {
						return drainowner.ErrJournal
					}
				}
				return nil
			}
			return owner.flushLocked(ctx, "", nil)
		})
		if cause != nil {
			localErr = errors.Join(localErr, cause)
		}
	}
	owner.mu.Lock()
	tx, external := owner.tx, owner.externalTx
	owner.mu.Unlock()
	if external {
		state = xhandler.TransactionCallerPending
	}
	if cause != nil {
		if tx != nil && !external {
			state = xhandler.TransactionRollbackUnknown
			if rollbackErr := completeOperation("DML completion rollback", tx.Rollback); rollbackErr != nil {
				localErr = errors.Join(localErr, rollbackErr)
				return errors.Join(cause, rollbackErr)
			}
			state = xhandler.TransactionRolledBack
		}
		return cause
	}
	if tx != nil && !external {
		state = xhandler.TransactionCommitUnknown
		if err := completeOperation("DML completion commit", tx.Commit); err != nil {
			localErr = errors.Join(localErr, err)
			return err
		}
		state = xhandler.TransactionCommitted
		if owner.onCommit != nil {
			if err := completeOperation("DML post-commit observer", func() error { owner.onCommit(ctx); return nil }); err != nil {
				localErr = errors.Join(localErr, err)
				return err
			}
		}
	}
	return nil
}

// Completion stays with the existing owner: a flush failure can roll back,
// while an attempted commit or rollback is never repeated after a panic.
func completeOperation(operation string, run func() error) (err error) {
	defer func() {
		if value := recover(); value != nil {
			err = dexec.NewPanicError(operation, value)
		}
	}()
	return run()
}
