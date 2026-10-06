package dml

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"reflect"
	"strings"

	rhandler "github.com/viant/datly/runtime/handler"
	"github.com/viant/sqlx/metadata/info"
	"github.com/viant/sqlx/option"
	xhandler "github.com/viant/xdatly/handler"
)

func (d *Data) executePlanStep(ctx context.Context, db *sql.DB, tx *sql.Tx, step executionStep) error {
	switch step.kind {
	case dataOpInsert:
		return d.executeInsertStep(ctx, db, tx, step)
	case dataOpUpdate:
		return d.executeUpdateStep(ctx, db, tx, step)
	case dataOpDelete:
		return d.executeDeleteStep(ctx, db, tx, step)
	case dataOpExecute:
		return d.executeSQLStep(ctx, tx, step)
	default:
		return fmt.Errorf("unsupported data operation %q", step.kind)
	}
}

func (d *Data) executeInsertStep(ctx context.Context, db *sql.DB, tx *sql.Tx, step executionStep) error {
	metric := step.beginMetric(ctx)
	service, err := d.inserter(ctx, db, step.table)
	if err != nil {
		return err
	}
	dialect, err := d.dialectFor(ctx, db)
	if err != nil {
		return err
	}
	if len(step.operations) > 1 && supportsInsertBatching(dialect) {
		if canBatchInsert(step.operations) {
			payload := batchInsertPayload(step.operations)
			options := buildExecutionOptions(tx, db, boundedInsertBatchSize(len(step.operations)))
			count, _, err := service.Exec(ctx, payload, options...)
			if d.insertConnectionRetryEligible(ctx, dialect, err) {
				count, _, err = service.Exec(ctx, payload, options...)
			}
			d.recordMutation(step, len(step.operations), count, err)
			metric.complete(count, err)
			return err
		}
	}
	options := buildExecutionOptions(tx, db, 0)
	for i, operation := range step.operations {
		if operation.queueContract == rhandler.SourceRow {
			options = buildExecutionOptions(tx, db, 1)
		} else if operation.queueContract == rhandler.SourceSlice && supportsInsertBatching(dialect) {
			options = buildExecutionOptions(tx, db, boundedInsertBatchSize(reflect.ValueOf(operation.data).Len()))
		}
		if i > 0 {
			metric.restart()
		}
		count, _, err := service.Exec(ctx, operation.data, options...)
		if d.insertConnectionRetryEligible(ctx, dialect, err) {
			count, _, err = service.Exec(ctx, operation.data, options...)
		}
		d.recordMutation(step, mutationRecordCount(operation.data), count, err)
		metric.complete(count, err)
		if err != nil {
			return err
		}
	}
	return nil
}

func (d *Data) executeUpdateStep(ctx context.Context, db *sql.DB, tx *sql.Tx, step executionStep) error {
	metric := step.beginMetric(ctx)
	service, err := d.updater(ctx, db, step.table)
	if err != nil {
		return err
	}
	options := buildExecutionOptions(tx, db, 0)
	for i, operation := range step.operations {
		if i > 0 {
			metric.restart()
		}
		writeOptions := options
		if operation.match != nil {
			writeOptions = append(append([]option.Option(nil), options...), option.IfMatch{Column: operation.match.Column, Value: operation.match.Value})
		}
		if operation.criteria != nil {
			writeOptions = append(append([]option.Option(nil), writeOptions...), operation.criteria)
		}
		count, err := service.Exec(ctx, operation.data, writeOptions...)
		if (operation.match != nil || operation.criteria != nil) && errors.Is(err, option.ErrNoMatch) {
			err = &xhandler.Conflict{Entity: step.table, Field: operationConflictField(operation), Reason: "mutation predicate no longer matches persisted row"}
		}
		if err == nil && (operation.match != nil || operation.criteria != nil) && count != 1 {
			err = &xhandler.Conflict{Entity: step.table, Field: operationConflictField(operation), Reason: "mutation predicate no longer matches persisted row"}
		}
		d.recordMutation(step, mutationRecordCount(operation.data), count, err)
		metric.complete(count, err)
		if err != nil {
			return err
		}
	}
	return nil
}

func (d *Data) executeDeleteStep(ctx context.Context, db *sql.DB, tx *sql.Tx, step executionStep) error {
	metric := step.beginMetric(ctx)
	service, err := d.deleter(ctx, db, step.table)
	if err != nil {
		return err
	}
	options := buildExecutionOptions(tx, db, 0)
	for i, operation := range step.operations {
		if i > 0 {
			metric.restart()
		}
		writeOptions := options
		if operation.match != nil {
			writeOptions = append(append([]option.Option(nil), options...), option.IfMatch{Column: operation.match.Column, Value: operation.match.Value})
		}
		if operation.criteria != nil {
			writeOptions = append(append([]option.Option(nil), writeOptions...), operation.criteria)
		}
		count, err := service.Exec(ctx, operation.data, writeOptions...)
		if (operation.match != nil || operation.criteria != nil) && errors.Is(err, option.ErrNoMatch) {
			err = &xhandler.Conflict{Entity: step.table, Field: operationConflictField(operation), Reason: "mutation predicate no longer matches persisted row"}
		}
		if err == nil && (operation.match != nil || operation.criteria != nil) && count != 1 {
			err = &xhandler.Conflict{Entity: step.table, Field: operationConflictField(operation), Reason: "mutation predicate no longer matches persisted row"}
		}
		d.recordMutation(step, mutationRecordCount(operation.data), count, err)
		metric.complete(count, err)
		if err != nil {
			return err
		}
	}
	return nil
}

func (d *Data) executeSQLStep(ctx context.Context, tx *sql.Tx, step executionStep) error {
	for _, operation := range step.operations {
		if _, err := tx.ExecContext(ctx, operation.dml, operation.args...); err != nil {
			return err
		}
	}
	return nil
}

func buildExecutionOptions(tx *sql.Tx, db *sql.DB, batchSize int) []option.Option {
	options := []option.Option{tx, db}
	if batchSize > 0 {
		options = append(options, option.BatchSize(batchSize))
	}
	return options
}

func operationConflictField(operation *dataOperation) string {
	if operation.match != nil {
		return operation.match.Column
	}
	return "predicate"
}

// insertConnectionRetryEligible preserves the source's dialect and successful
// mutation gates. The failed statement is retried at most once in this call.
// Retaining the owned transaction avoids legacy's stale-option/new-Tx split.
func (d *Data) insertConnectionRetryEligible(ctx context.Context, dialect *info.Dialect, cause error) bool {
	if cause == nil || ctx.Err() != nil || !strings.Contains(cause.Error(), "invalid connection") || !supportsInsertBatching(dialect) {
		return false
	}
	owner := d.owner()
	owner.mu.Lock()
	defer owner.mu.Unlock()
	for _, result := range owner.mutationResults {
		if result.Error == nil && result.Affected > 0 && (result.Operation == string(dataOpInsert) || result.Operation == string(dataOpUpdate)) {
			return false
		}
	}
	return true
}
