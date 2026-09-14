package transcribe

import (
	"reflect"

	gen "github.com/viant/datly/transcribe/generate"
	plan "github.com/viant/datly/transcribe/handler/ast"
	"github.com/viant/sqlx/io"
	"github.com/viant/sqlx/io/insert"
	"github.com/viant/sqlx/io/update"
	xshape "github.com/viant/x/shape"
)

// Native write effects are source-enrichment facts. The neutral handler
// compiler and generated public policy never import SQLX execution packages.
func (g *handlerGeneration) refineLateWriteEffects(record *plan.RecordPlan, generated *gen.Plan, valueType string) error {
	if record.Auxiliary || record.Entity == nil {
		return nil
	}
	base, err := g.recordBase(valueType, record.Cardinality)
	if err != nil {
		return err
	}
	effects := plan.LateWriteEffects{}
	seen := map[string]bool{}
	addDefault := func(name string, tags reflect.StructTag) {
		if io.ParseTag(tags).HasDefaultGenerator() && !seen[name] {
			effects.DefaultFields = append(effects.DefaultFields, name)
			seen[name] = true
		}
	}
	owned := false
	if view := generated.ViewByType(base); view != nil && view.Ownership == gen.ViewGenerated {
		owned = true
		for _, field := range view.Fields {
			addDefault(field.Name, reflect.StructTag(field.Tag))
		}
	}
	if g.input.TypeResolver == nil {
		if !owned {
			effects.Unresolved = "canonical entity type authority is required"
		}
		record.Entity.LateWrite = effects
		return nil
	}
	resolved, err := g.input.TypeResolver.ResolveShape(base)
	if err != nil || resolved == nil || resolved.Descriptor == nil {
		if !owned {
			effects.Unresolved = "canonical entity native effects could not be resolved"
		}
		record.Entity.LateWrite = effects
		return nil
	}
	shape := xshape.New(resolved.Descriptor, g.input.TypeResolver.Descriptor)
	if effects.InsertHook, err = shape.Implements(reflect.TypeOf((*insert.Insertable)(nil)).Elem(), true); err != nil {
		effects.Unresolved = err.Error()
		record.Entity.LateWrite = effects
		return nil
	}
	if effects.UpdateHook, err = shape.Implements(reflect.TypeOf((*update.Updatable)(nil)).Elem(), true); err != nil {
		effects.Unresolved = err.Error()
		record.Entity.LateWrite = effects
		return nil
	}
	fields, err := shape.Fields()
	if err != nil {
		effects.Unresolved = err.Error()
		record.Entity.LateWrite = effects
		return nil
	}
	for _, field := range fields {
		if field.Exported {
			addDefault(field.Name, field.Tag)
		}
	}
	record.Entity.LateWrite = effects
	return nil
}
