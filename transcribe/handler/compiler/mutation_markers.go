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
	return nil
}
