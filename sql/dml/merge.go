package dml

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"reflect"
	"strings"

	sqlconfig "github.com/viant/sqlx/io/config"
	"github.com/viant/sqlx/metadata/info"
	"github.com/viant/sqlx/moption"
)

// ErrMergeUncommittedPrefix prevents a SQLX destination fetch from silently
// ignoring writes already executed in the same transaction. A separate owned
// component transaction can preserve an independently committed merge boundary.
var ErrMergeUncommittedPrefix = errors.New("SQLX merge destination fetch cannot observe an uncommitted DML prefix")

// Merge buffers SQLX's dialect-specific merge, preserving its configuration,
// allocator and result. Flush executes it; only the DML owner completes its Tx.
func (d *Data) Merge(table string, rows any, config info.MergeConfig, result *info.MergeResult, options ...moption.Option) error {
	if strings.TrimSpace(table) == "" {
		return fmt.Errorf("merge table is required")
	}
	if config == nil || (reflect.ValueOf(config).Kind() == reflect.Pointer && reflect.ValueOf(config).IsNil()) {
		return fmt.Errorf("merge config is required")
	}
	if result == nil {
		return fmt.Errorf("merge result destination is required")
	}
	for _, option := range options {
		if option == nil {
			return fmt.Errorf("merge option must not be nil")
		}
	}
	if moption.NewOptions(options...).GetTransaction() != nil {
		return fmt.Errorf("merge transaction must be supplied by the DML owner")
	}
	return d.append(dataOperation{kind: dataOpMerge, table: table, data: rows,
		mergeConfig: config, mergeOptions: append([]moption.Option(nil), options...), mergeResult: result})
}

func (d *Data) executeMergeStep(ctx context.Context, db *sql.DB, tx *sql.Tx, step executionStep) error {
	metric := step.beginMetric(ctx)
	for i, operation := range step.operations {
		if i > 0 {
			metric.restart()
		}
		if d.mergeHasUncommittedPrefix(tx) {
			d.recordMutation(step, mutationRecordCount(operation.data), 0, ErrMergeUncommittedPrefix)
			metric.complete(0, ErrMergeUncommittedPrefix)
			return ErrMergeUncommittedPrefix
		}
		dialect, err := d.dialectFor(ctx, db)
		var result info.MergeResult
		if err == nil {
			service, resolveErr := sqlconfig.MergeExecutor(dialect, operation.mergeConfig)
			err = resolveErr
			if err == nil {
				options := append(append([]moption.Option(nil), operation.mergeOptions...), moption.WithTransaction(tx))
				result, err = service.Exec(ctx, operation.data, db, step.table, options...)
			}
		}
		*operation.mergeResult = result
		var affected int64
		if result != nil {
			affected = int64(result.RowsAffected())
		}
		d.recordMutation(step, mutationRecordCount(operation.data), affected, err)
		metric.complete(affected, err)
		if err != nil {
			return err
		}
	}
	return nil
}

func (d *Data) mergeHasUncommittedPrefix(tx *sql.Tx) bool {
	owner := d.owner()
	owner.mu.Lock()
	defer owner.mu.Unlock()
	for _, operation := range flattenData(owner) {
		if operation.executed && operation.executionTx == tx {
			return true
		}
	}
	return false
}
