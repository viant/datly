package writer

import (
	"context"
	"fmt"
	"strings"

	"github.com/viant/datly/internal/dialectcontext"
	"github.com/viant/sqlx/metadata/info"
	h "github.com/viant/xdatly/handler"
)

// Batches contain only adjacent, authoritative deletes from one parent and role.
// Each logical occurrence still traverses queue observation and AfterQueue.
func (p *Program) reconciliationDeleteBatches(ctx context.Context, binder h.Binder) (map[*Action]func() error, error) {
	result := map[*Action]func() error{}
	if p.metadata.Root.reconciliation == nil {
		return result, nil
	}
	var dml h.DML
	var dialect *info.Dialect
	for start := 0; start < len(p.actions.Rows); {
		first := p.actions.Rows[start]
		frame := p.actionFrame(first)
		var role *reconciliationRoleMetadata
		if frame != nil && frame.Parent != nil && first.Kind == h.WriteDelete {
			for i := range p.metadata.Root.reconciliation.roles {
				candidate := &p.metadata.Root.reconciliation.roles[i]
				if candidate.relation.Child == frame.Record && candidate.declaration.DeleteBatchSize > 0 {
					role = candidate
					break
				}
			}
		}
		if role == nil {
			start++
			continue
		}
		end := start + 1
		for end < len(p.actions.Rows) && end-start < role.declaration.DeleteBatchSize {
			action := p.actions.Rows[end]
			other := p.actionFrame(action)
			if other == nil || action.Kind != h.WriteDelete || other.Record != frame.Record || other.Parent != frame.Parent {
				break
			}
			end++
		}
		if dml == nil {
			var err error
			dml, err = lookup[h.DML](ctx, binder, h.DMLKey)
			if err != nil {
				return nil, err
			}
			dialectCtx, err := mutationPredicateContext(ctx, binder)
			if err != nil {
				return nil, err
			}
			dialect = dialectcontext.Dialect(dialectCtx)
			if dialect == nil {
				return nil, fmt.Errorf("reconciliation delete batching requires SQL dialect")
			}
		}
		statement, args, err := p.reconciliationDeleteSQL(dialect, role, p.actions.Rows[start:end])
		if err != nil {
			return nil, err
		}
		// The first occurrence queues the statement at the original role position.
		// The remaining occurrences retain their logical queue notifications/hooks.
		result[first] = func() error { return dml.Execute(statement, args...) }
		for _, action := range p.actions.Rows[start+1 : end] {
			result[action] = func() error { return nil }
		}
		start = end
	}
	return result, nil
}

func (p *Program) reconciliationDeleteSQL(dialect *info.Dialect, role *reconciliationRoleMetadata, actions []*Action) (string, []any, error) {
	if len(actions) == 0 {
		return "", nil, fmt.Errorf("empty reconciliation delete batch")
	}
	first := p.actionFrame(actions[0])
	if first == nil || first.Parent == nil || len(first.Record.Keys) != 1 {
		return "", nil, fmt.Errorf("invalid reconciliation delete batch")
	}
	table, err := dialect.TableIdentifier(first.Record.Table, "")
	if err != nil {
		return "", nil, err
	}
	key, err := dialect.MappedColumnIdentifier(first.Record.Keys[0].Column)
	if err != nil {
		return "", nil, err
	}
	var predicates []string
	var args []any
	for _, link := range role.relation.Links {
		column, err := dialect.MappedColumnIdentifier(link.Child.Column)
		if err != nil {
			return "", nil, err
		}
		predicates = append(predicates, column+" = ?")
		args = append(args, first.Parent.Entity.Elem().FieldByIndex(link.Parent.Index).Interface())
	}
	for _, action := range actions {
		frame := p.actionFrame(action)
		if frame == nil || frame.Parent != first.Parent || frame.Record != first.Record || action.Kind != h.WriteDelete {
			return "", nil, fmt.Errorf("mixed reconciliation delete batch")
		}
		if err := p.requireReconciliationCurrent(role.relation, first.Parent, frame.Previous, true); err != nil {
			return "", nil, err
		}
		args = append(args, frame.Entity.Elem().FieldByIndex(frame.Record.Keys[0].Index).Interface())
	}
	predicates = append(predicates, key+" IN ("+strings.TrimSuffix(strings.Repeat("?,", len(actions)), ",")+")")
	return dialect.EnsurePlaceholders("DELETE FROM " + table + " WHERE " + strings.Join(predicates, " AND ")), args, nil
}
