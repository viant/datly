package dml

import (
	"errors"
	"github.com/go-sql-driver/mysql"
	dexec "github.com/viant/datly/exec"
	"reflect"
	"strings"
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

func (d *Data) TransactionContention(err error) bool {
	if err == nil {
		return false
	}
	if joined, ok := err.(interface{ Unwrap() []error }); ok {
		parts := joined.Unwrap()
		if len(parts) == 0 {
			return false
		}
		for _, part := range parts {
			if !d.TransactionContention(part) {
				return false
			}
		}
		return true
	}
	if wrapped, ok := err.(interface{ Unwrap() error }); ok {
		return d.TransactionContention(wrapped.Unwrap())
	}
	owner := d.owner()
	name := ""
	if owner.dialect != nil {
		name = owner.dialect.Name
	}
	return mutationContention(err) || codedSQLiteContention(err, name)
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
	dialectName := ""
	if d.dialect != nil {
		dialectName = d.dialect.Name
	}
	owner.mutationResults = append(owner.mutationResults, dexec.MutationResult{Operation: string(step.kind), Table: step.table, Records: records, Affected: affected, Error: err, Contention: mutationContention(err) || codedSQLiteContention(err, dialectName)})
}

// modernc-style SQLite drivers expose extended result codes through Code().
// Interpret those codes only with a resolved SQLite dialect; an arbitrary
// provider's numeric code must not be mistaken for SQLite lock contention.
func codedSQLiteContention(err error, dialectName string) bool {
	if !strings.EqualFold(dialectName, "SQLite") {
		return false
	}
	var coded interface{ Code() int }
	if !errors.As(err, &coded) {
		return false
	}
	primary := coded.Code() & 0xff
	return primary == 5 || primary == 6 // SQLITE_BUSY / SQLITE_LOCKED
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
	return sqliteMutationContention(err)
}
