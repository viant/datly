package dml

import (
	"errors"
	"github.com/go-sql-driver/mysql"
	"github.com/mattn/go-sqlite3"
	dexec "github.com/viant/datly/exec"
	"reflect"
)

func (d *Data) MutationReport() dexec.MutationReport {
	owner := d.owner()
	owner.mu.Lock()
	defer owner.mu.Unlock()
	report := dexec.MutationReport{Queued: int(owner.nextOp), Results: append([]dexec.MutationResult(nil), owner.mutationResults...)}
	for _, operation := range flattenData(owner) {
		if operation.frame != owner {
			report.Nested = true
			break
		}
	}
	return report
}

func mutationRecordCount(data any) int {
	value := reflect.ValueOf(data)
	for value.IsValid() && (value.Kind() == reflect.Pointer || value.Kind() == reflect.Interface) {
		if value.IsNil() {
			return 0
		}
		value = value.Elem()
	}
	if !value.IsValid() {
		return 0
	}
	if value.Kind() == reflect.Slice || value.Kind() == reflect.Array {
		return value.Len()
	}
	return 1
}

func (d *Data) recordMutation(step executionStep, records int, affected int64, err error) {
	owner := d.owner()
	owner.mu.Lock()
	defer owner.mu.Unlock()
	owner.mutationResults = append(owner.mutationResults, dexec.MutationResult{Operation: string(step.kind), Table: step.table, Records: records, Affected: affected, Error: err, Contention: mutationContention(err)})
}

func mutationContention(err error) bool {
	var state interface{ SQLState() string }
	if errors.As(err, &state) {
		return state.SQLState() == "40001" || state.SQLState() == "40P01"
	}
	var mysqlError *mysql.MySQLError
	if errors.As(err, &mysqlError) {
		return mysqlError.Number == 1213 || mysqlError.Number == 1205
	}
	var sqliteError sqlite3.Error
	if errors.As(err, &sqliteError) {
		return sqliteError.Code == sqlite3.ErrBusy || sqliteError.Code == sqlite3.ErrLocked
	}
	return false
}
