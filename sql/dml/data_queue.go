package dml

import (
	"errors"
	"strings"
)

type dataOperationKind string

const (
	dataOpInsert  dataOperationKind = "insert"
	dataOpUpdate  dataOperationKind = "update"
	dataOpDelete  dataOperationKind = "delete"
	dataOpExecute dataOperationKind = "execute"
)

type dataOperation struct {
	id       uint64
	frame    *Data
	kind     dataOperationKind
	table    string
	data     any
	dml      string
	args     []any
	executed bool
	reserved bool
}

func (d *Data) Insert(tableName string, data any) error {
	return d.append(dataOperation{kind: dataOpInsert, table: tableName, data: data})
}

func (d *Data) Update(tableName string, data any) error {
	return d.append(dataOperation{kind: dataOpUpdate, table: tableName, data: data})
}

func (d *Data) Delete(tableName string, data any) error {
	return d.append(dataOperation{kind: dataOpDelete, table: tableName, data: data})
}

func (d *Data) Execute(dml string, args ...any) error {
	return d.append(dataOperation{kind: dataOpExecute, dml: dml, args: append([]any(nil), args...)})
}

func (d *Data) append(operation dataOperation) error {
	owner := d.owner()
	owner.mu.Lock()
	defer owner.mu.Unlock()
	if err := owner.appendableLocked(); err != nil {
		return err
	}
	if !d.open {
		return ErrComponentSealed
	}
	owner.nextOp++
	operation.id = owner.nextOp
	operation.frame = d
	d.queue = append(d.queue, &operation)
	return nil
}

func (d *Data) operations(tableName string, target *Data) []*dataOperation {
	owner := d.owner()
	owner.mu.Lock()
	defer owner.mu.Unlock()
	all := flattenData(owner)
	if target == nil && tableName == "" {
		return pendingOperations(all)
	}
	pending := make([]*dataOperation, 0, len(all))
	last := -1
	for _, operation := range all {
		if operation.executed {
			continue
		}
		pending = append(pending, operation)
		if operation.frame == target && (tableName == "" || strings.EqualFold(operation.table, tableName)) {
			last = len(pending) - 1
		}
	}
	if last < 0 {
		return nil
	}
	return append([]*dataOperation(nil), pending[:last+1]...)
}

func pendingOperations(operations []*dataOperation) []*dataOperation {
	result := make([]*dataOperation, 0, len(operations))
	for _, operation := range operations {
		if !operation.executed {
			result = append(result, operation)
		}
	}
	return result
}

func (d *Data) appendableLocked() error {
	if d.completed {
		return ErrInvocationCompleted
	}
	if d.failed != nil {
		return errors.Join(ErrInvocationFailed, d.failed)
	}
	return nil
}
