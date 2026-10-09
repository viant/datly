package dml

import (
	"errors"
	"strings"

	"github.com/viant/datly/internal/drainowner"
	rhandler "github.com/viant/datly/runtime/handler"
	"github.com/viant/sqlx"
	xhandler "github.com/viant/xdatly/handler"
)

type dataOperationKind string

const (
	dataOpInsert  dataOperationKind = "insert"
	dataOpUpdate  dataOperationKind = "update"
	dataOpDelete  dataOperationKind = "delete"
	dataOpExecute dataOperationKind = "execute"
)

type dataOperation struct {
	queueContract   rhandler.QueueContract
	appendBarrier   bool
	payloadEvidence *queuePayloadEvidence
	id              uint64
	frame           *Data
	journalFrame    *drainowner.Frame
	kind            dataOperationKind
	table           string
	data            any
	dml             string
	args            []any
	match           *xhandler.Match
	criteria        *sqlx.Criteria
	executed        bool
	reserved        bool
}

func (d *Data) Insert(tableName string, data any) error {
	return d.append(dataOperation{kind: dataOpInsert, table: tableName, data: data})
}

func (d *Data) Update(tableName string, data any) error {
	return d.UpdateWithOptions(tableName, data)
}

func (d *Data) UpdateWithOptions(tableName string, data any, options ...xhandler.Option) error {
	condition := writeMatch(options)
	return d.append(dataOperation{kind: dataOpUpdate, table: tableName, data: data, match: condition})
}

func (d *Data) Delete(tableName string, data any) error {
	return d.DeleteWithOptions(tableName, data)
}

func (d *Data) DeleteWithOptions(tableName string, data any, options ...xhandler.Option) error {
	condition := writeMatch(options)
	return d.append(dataOperation{kind: dataOpDelete, table: tableName, data: data, match: condition})
}

// UpdateWithCriteria queues an existing predicate result in the managed unit.
func (d *Data) UpdateWithCriteria(tableName string, data any, criteria *sqlx.Criteria, options ...xhandler.Option) error {
	return d.append(dataOperation{kind: dataOpUpdate, table: tableName, data: data, match: writeMatch(options), criteria: criteria.Clone()})
}

// DeleteWithCriteria queues an existing predicate result in the managed unit.
func (d *Data) DeleteWithCriteria(tableName string, data any, criteria *sqlx.Criteria, options ...xhandler.Option) error {
	return d.append(dataOperation{kind: dataOpDelete, table: tableName, data: data, match: writeMatch(options), criteria: criteria.Clone()})
}

func writeMatch(options []xhandler.Option) *xhandler.Match {
	settings := &xhandler.Options{}
	for _, apply := range options {
		if apply != nil {
			apply(settings)
		}
	}
	return settings.IfMatch
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
	operation.journalFrame = d.journalFrame
	if err := drainowner.AppendJournal(owner, d.journalFrame, &operation); err != nil {
		owner.nextOp--
		return owner.failProtectedMutationLocked(err)
	}
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
	if err := drainowner.CheckPrefixMutation(d); err != nil {
		return err
	}
	if err := drainowner.ProtectedOwnerFailure(d); err != nil {
		return err
	}
	if d.mutationAdmissionClosed {
		return d.failProtectedMutationLocked(ErrMutationAdmissionClosed)
	}
	if d.completed {
		return ErrInvocationCompleted
	}
	if d.failed != nil {
		return errors.Join(ErrInvocationFailed, d.failed)
	}
	return nil
}
