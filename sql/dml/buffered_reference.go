package dml

import (
	"context"
	"fmt"
	"reflect"
	"strings"

	"github.com/viant/datly/sql/validation"
	"github.com/viant/sqlx/io"
	xhandler "github.com/viant/xdatly/handler"
)

// prefixBeforeComponent follows the same traversal as flattenData. An append
// sequence number is not execution order, especially for binding children.
func prefixBeforeComponent(root, target *Data) ([]*dataOperation, bool) {
	if root == target {
		return nil, true
	}
	var prefix []*dataOperation
	for i := 0; i <= len(root.queue); i++ {
		for _, marker := range root.markers {
			if marker.at != i {
				continue
			}
			part, found := prefixBeforeComponent(marker.child, target)
			prefix = append(prefix, part...)
			if found {
				return prefix, true
			}
		}
		if i < len(root.queue) {
			prefix = append(prefix, root.queue[i])
		}
	}
	for _, child := range root.bindings {
		part, found := prefixBeforeComponent(child, target)
		prefix = append(prefix, part...)
		if found {
			return prefix, true
		}
	}
	return prefix, false
}

func (d *Data) bufferedParents() []validation.BufferedParent {
	owner := d.owner()
	owner.mu.Lock()
	defer owner.mu.Unlock()
	if !owner.invocation || owner.tx == nil {
		return nil
	}
	prefix, found := prefixBeforeComponent(owner, d)
	if !found {
		return nil
	}
	var result []validation.BufferedParent
	for _, operation := range prefix {
		if operation.kind != dataOpInsert || operation.executed {
			continue
		}
		// Opaque SQL and mutable DML cannot prove continued existence. Keep
		// normal database validation when the prefix contains such a barrier.
		if err := d.checkBufferedParentLocked(operation, ""); err != nil {
			continue
		}
		at, count, err := io.Values(operation.data)
		if err != nil {
			continue
		}
		for i := 0; i < count; i++ {
			row := at(i)
			value := reflect.ValueOf(row)
			if !value.IsValid() || value.Kind() != reflect.Pointer || value.IsNil() || value.Elem().Kind() != reflect.Struct {
				continue
			}
			transaction := owner.tx
			index := i
			result = append(result, validation.BufferedParent{Table: operation.table, Value: row, Check: func(ctx context.Context, ref xhandler.ValidationReference) error {
				if err := ctx.Err(); err != nil {
					return err
				}
				owner.mu.Lock()
				defer owner.mu.Unlock()
				if owner.tx != transaction {
					return fmt.Errorf("buffered reference transaction changed")
				}
				if err := d.checkBufferedParentLocked(operation, ref.Column); err != nil {
					return err
				}
				current, size, err := io.Values(operation.data)
				if err != nil {
					return err
				}
				if index >= size || reflect.ValueOf(current(index)) != value {
					return fmt.Errorf("buffered parent payload replaced")
				}
				return nil
			}})
		}
	}
	return result
}

func (d *Data) checkBufferedParentLocked(parent *dataOperation, referenceColumn string) error {
	owner := d.owner()
	prefix, found := prefixBeforeComponent(owner, d)
	matched := false
	for _, operation := range prefix {
		if operation == parent {
			matched = true
			break
		}
	}
	if !found || !matched || parent.kind != dataOpInsert {
		return fmt.Errorf("buffered parent no longer precedes child")
	}
	seen := false
	// The proof covers the path to this consumer and its complete subtree.
	// Caller work queued after the component executes later and cannot alter
	// the reference before this consumer uses it. Keep checking the subtree
	// itself, including barriers appended after validation or its INSERT.
	for _, operation := range append(prefix, flattenData(d)...) {
		if operation == parent {
			seen = true
			continue
		}
		if seen && operation.kind != dataOpInsert {
			if operation.kind == dataOpUpdate && (referenceColumn == "" || updatePreservesReference(operation.data, referenceColumn)) {
				continue
			}
			return fmt.Errorf("buffered parent proof invalidated by %s", operation.kind)
		}
	}
	return nil
}

// SQLX puts primary keys in the WHERE suffix, never the SET list. Inspect
// the actual update shape: the inserted parent shape is not update authority.
// This deliberately does not infer disjoint tables from spelling or schema.
func updatePreservesReference(payload any, column string) bool {
	at, count, err := io.Values(payload)
	if err != nil || count == 0 {
		return false
	}
	for i := 0; i < count; i++ {
		columns, _, err := io.StructColumnMapper(at(i))
		if err != nil {
			return false
		}
		keys := io.Columns(columns).PrimaryKeys()
		if keys < 0 {
			return false
		}
		found := false
		matches := 0
		for j, mapped := range columns {
			if !strings.EqualFold(mapped.Name(), column) {
				continue
			}
			matches++
			if mapped.Name() == column && j >= keys && mapped.Tag() != nil && mapped.Tag().PrimaryKey {
				found = true
			}
		}
		// A typed projection cannot assign a column absent from its mapping.
		// This also covers same-table updates; no table-name inference is needed.
		if matches == 0 {
			continue
		}
		if matches != 1 {
			return false
		}
		if !found {
			return false
		}
	}
	return true
}
