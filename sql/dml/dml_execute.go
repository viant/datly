package dml

import (
	"context"
	"database/sql"
	"errors"
	"fmt"

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
			metric.complete(count, err)
			return err
		}
	}
	options := buildExecutionOptions(tx, db, 0)
	for i, operation := range step.operations {
		if i > 0 {
			metric.restart()
		}
		count, _, err := service.Exec(ctx, operation.data, options...)
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
		count, err := service.Exec(ctx, operation.data, writeOptions...)
		if operation.match != nil && errors.Is(err, option.ErrNoMatch) {
			err = &xhandler.Conflict{Entity: step.table, Field: operation.match.Column, Reason: "expected token no longer matches persisted row"}
		}
		if err == nil && operation.match != nil && count != 1 {
			err = &xhandler.Conflict{Entity: step.table, Field: operation.match.Column, Reason: "expected token no longer matches persisted row"}
		}
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
		count, err := service.Exec(ctx, operation.data, writeOptions...)
		if operation.match != nil && errors.Is(err, option.ErrNoMatch) {
			err = &xhandler.Conflict{Entity: step.table, Field: operation.match.Column, Reason: "expected token no longer matches persisted row"}
		}
		if err == nil && operation.match != nil && count != 1 {
			err = &xhandler.Conflict{Entity: step.table, Field: operation.match.Column, Reason: "expected token no longer matches persisted row"}
		}
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
