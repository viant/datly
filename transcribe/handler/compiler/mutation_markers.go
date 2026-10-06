package compiler

import (
	"fmt"

	"github.com/viant/datly/spec"
	plan "github.com/viant/datly/transcribe/handler/ast"
	"github.com/viant/datly/typecatalog"
)

// mutationMarkers compiles explicit column policy into the target-neutral write
// decision. Shape refinement may subsequently qualify its Go field and type.
func (c *compiler) mutationMarkers(record *plan.RecordPlan, view *spec.View, operation plan.Operation) error {
	for _, column := range view.Columns {
		if column == nil || (!column.DeleteMarker && !column.ConcurrencyToken) {
			continue
		}
		if record.Auxiliary || operation == plan.OperationPost || record.Current == nil {
			return fmt.Errorf("mutation marker %s requires a writable update role with authorized Previous", column.Name)
		}
		if column.PrimaryKey || column.DeleteMarker && column.ConcurrencyToken {
			return fmt.Errorf("mutation marker %s cannot be an identity or both policies", column.Name)
		}
		target := &record.Write.ConcurrencyToken
		if column.DeleteMarker {
			target = &record.Write.DeleteMarker
		}
		if target.Field != "" {
			return fmt.Errorf("mutation marker is duplicated for view %s", view.Name)
		}
		*target = plan.FieldRef{Field: typecatalog.FieldName(column.Name), Source: column.Source, Type: column.EffectiveType()}
		if column.DeleteMarker {
			record.Write.Allowed = append(record.Write.Allowed, plan.ActionDelete)
		}
	}
	if view.QueueContract != "" {
		if view.QueueContract != "source-row" || record.Auxiliary || record.Table == "" || (operation != plan.OperationPost && operation != plan.OperationPatch) || record.Write.ConcurrencyToken.Field != "" || view.MutationPredicateGroup != nil {
			return fmt.Errorf("queue_contract source-row requires a POST/PATCH physical role without matched/criteria options; source-slice authoring is not yet available")
		}
		record.Write.QueueContract = view.QueueContract
	}
	return compileWriterActionPolicy(record, view, operation)
}

func compileWriterActionPolicy(record *plan.RecordPlan, view *spec.View, operation plan.Operation) error {
	if view.WriterActionPolicy == "" {
		return nil
	}
	if view.WriterActionPolicy != "insert-delete" {
		return fmt.Errorf("writer_action_policy must be insert-delete")
	}
	if operation != plan.OperationPatch || record.Auxiliary || record.Table == "" || record.Current == nil || len(record.Keys) == 0 || record.Write.DeleteMarker.Field == "" {
		return fmt.Errorf("writer_action_policy insert-delete requires a PATCH physical leaf with Current, keys and delete marker")
	}
	if len(view.Relations) != 0 || len(record.SelfRelations) != 0 {
		return fmt.Errorf("writer_action_policy insert-delete requires a leaf view")
	}
	if view.WriterIdentityPolicy != "" || view.MutationPredicateGroup != nil || record.Write.ConcurrencyToken.Field != "" {
		return fmt.Errorf("writer_action_policy insert-delete does not support identity overrides, concurrency tokens or mutation predicates")
	}
	record.Write.ActionPolicy = view.WriterActionPolicy
	record.Write.Existing = plan.ActionInsert
	record.Write.Missing = plan.ActionInsert
	record.Write.Allowed = []plan.Action{plan.ActionInsert, plan.ActionDelete}
	return nil
}
