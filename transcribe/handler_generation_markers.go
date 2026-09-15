package transcribe

import (
	"fmt"

	"github.com/viant/datly/spec"
	gen "github.com/viant/datly/transcribe/generate"
	plan "github.com/viant/datly/transcribe/handler/ast"
)

func (g *handlerGeneration) refineMutationMarkers(record *plan.RecordPlan, generated *gen.Plan, valueType string) error {
	if record.Write.DeleteMarker.Field == "" && record.Write.ConcurrencyToken.Field == "" {
		return nil
	}
	fields, err := g.currentProjectionFields(generated, valueType, record.Cardinality)
	if err != nil {
		return err
	}
	for _, policy := range []struct {
		ref      *plan.FieldRef
		deletion bool
	}{{&record.Write.DeleteMarker, true}, {&record.Write.ConcurrencyToken, false}} {
		if policy.ref.Field == "" {
			continue
		}
		field, err := fields.resolve(policy.ref.Field)
		if err != nil {
			return err
		}
		policy.ref.Field, policy.ref.Type = field.name, spec.TypeRef{Name: field.expression}
		found := false
		if record.Entity != nil {
			for i := range record.Entity.Fields {
				target := &record.Entity.Fields[i]
				if target.Name != field.name {
					continue
				}
				found = true
				target.DeleteMarker, target.ConcurrencyToken = policy.deletion, !policy.deletion
				if policy.deletion {
					target.Writable = false
				}
			}
		}
		if !found {
			return fmt.Errorf("mutation marker %s requires canonical entity presence metadata", field.name)
		}
	}
	return nil
}
