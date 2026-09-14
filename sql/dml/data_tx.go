package dml

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	xhandler "github.com/viant/xdatly/handler"
)

func (d *Data) database() (*sql.DB, error) {
	owner := d.owner()
	if owner.db == nil {
		return nil, fmt.Errorf("DML database is required")
	}
	return owner.db, nil
}

func (d *Data) transaction(ctx context.Context) (*sql.Tx, error) {
	owner := d.owner()
	owner.mu.Lock()
	defer owner.mu.Unlock()
	if owner.tx != nil {
		return owner.tx, nil
	}
	db, err := d.database()
	if err != nil {
		return nil, err
	}
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return nil, err
	}
	owner.tx = tx
	return tx, nil
}

func (d *Data) Flush(ctx context.Context, tableName string) error {
	return d.flush(ctx, tableName, d)
}

// PrepareFinalization makes local flush failures visible to an error-aware
// finalizer before commit. A supplied transaction retains its existing ordering:
// no queued writes execute until the finalizer has completed successfully.
func (d *Data) PrepareFinalization(ctx context.Context) error {
	owner := d.owner()
	owner.mu.Lock()
	external := owner.externalTx
	owner.mu.Unlock()
	if external {
		return nil
	}
	return owner.PrepareCompletion(ctx)
}

// PrepareCompletion drains the complete invocation journal into its owned or
// supplied transaction without completing that transaction. The engine uses
// this internal lifecycle seam to prepare every database unit before any local
// unit is committed.
func (d *Data) PrepareCompletion(ctx context.Context) error {
	owner := d.owner()
	owner.executionMu.Lock()
	defer owner.executionMu.Unlock()
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
	return owner.flushLocked(ctx, "", nil)
}

func (d *Data) flush(ctx context.Context, tableName string, target *Data) error {
	owner := d.owner()
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
	matched := owner.operations(tableName, target)
	if len(matched) == 0 {
		return nil
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
	owner := d.owner()
	owner.executionMu.Lock()
	defer owner.executionMu.Unlock()
	owner.SealComponent()
	owner.mu.Lock()
	if owner.completed {
		owner.mu.Unlock()
		return errors.Join(cause, ErrInvocationCompleted)
	}
	owner.completed = true
	failed := owner.failed
	owner.mu.Unlock()
	state := xhandler.TransactionNone
	localErr := failed
	defer func() { owner.recordOutcome(state, localErr) }()
	if failed != nil {
		cause = errors.Join(cause, failed)
	}
	if cause == nil {
		cause = owner.flushLocked(ctx, "", nil)
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
			if rollbackErr := tx.Rollback(); rollbackErr != nil {
				localErr = errors.Join(localErr, rollbackErr)
				return errors.Join(cause, rollbackErr)
			}
			state = xhandler.TransactionRolledBack
		}
		return cause
	}
	if tx != nil && !external {
		state = xhandler.TransactionCommitUnknown
		if err := tx.Commit(); err != nil {
			localErr = errors.Join(localErr, err)
			return err
		}
		state = xhandler.TransactionCommitted
		if owner.onCommit != nil {
			owner.onCommit(ctx)
		}
	}
	return nil
}
