package transcribe

import (
	"fmt"

	"github.com/viant/datly/spec"
	gen "github.com/viant/datly/transcribe/generate"
	plan "github.com/viant/datly/transcribe/handler/ast"
	"github.com/viant/datly/transcribe/handler/compiler"
	"github.com/viant/datly/typecatalog"
	xshape "github.com/viant/x/shape"
)

type currentFieldAuthority struct{ name, expression string }

func (g *handlerGeneration) refineCurrentProjection(record *plan.RecordPlan, generated *gen.Plan, valueType, currentType string) error {
	if record.Current == nil {
		return nil
	}
	entity, err := g.currentProjectionFields(generated, valueType, record.Cardinality)
	if err != nil {
		return err
	}
	current, err := g.currentProjectionFields(generated, currentType, spec.CardinalityMany)
	if err != nil {
		return err
	}
	refined := make([]plan.CurrentField, len(record.Current.Fields))
	for i, field := range record.Current.Fields {
		from, err := current.resolve(field.Current.Field)
		if err != nil {
			return fmt.Errorf("current view %s: %w", record.Current.ViewIdentity, err)
		}
		to, err := entity.resolve(field.Entity.Field)
		if err != nil {
			return fmt.Errorf("entity view %s: %w", record.Identity, err)
		}
		field.Current.Field, field.Entity.Field = from.name, to.name
		refined[i], err = (&compiler.Compiler{}).RefineCurrentField(field, from.expression, to.expression)
		if err != nil {
			return err
		}
	}
	keys := append([]plan.KeyPart(nil), record.Current.Keys...)
	for index, key := range record.Current.Keys {
		field, err := current.resolve(key.Field)
		if err != nil {
			return fmt.Errorf("current view %s identity: %w", record.Current.ViewIdentity, err)
		}
		key.Field, key.Type = field.name, spec.TypeRef{Name: field.expression}
		keys[index] = key
	}
	self := append([]plan.FieldRef(nil), record.Current.Self...)
	for index, holder := range record.Current.Self {
		field, err := current.resolve(holder.Field)
		if err != nil {
			return fmt.Errorf("current view %s self holder: %w", record.Current.ViewIdentity, err)
		}
		holder.Field, holder.Type = field.name, spec.TypeRef{Name: field.expression}
		self[index] = holder
	}
	record.Current.Fields, record.Current.Keys, record.Current.Self = refined, keys, self
	return nil
}

type currentFieldAuthorities []currentFieldAuthority

func (fields currentFieldAuthorities) resolve(name string) (currentFieldAuthority, error) {
	// Authored-name formatting belongs to typecatalog; structural enumeration
	// and source type identity have already been resolved by native shape.
	var result *currentFieldAuthority
	for i := range fields {
		if typecatalog.FieldName(fields[i].name) != typecatalog.FieldName(name) {
			continue
		}
		if result != nil {
			return currentFieldAuthority{}, fmt.Errorf("ambiguous canonical field %q", name)
		}
		result = &fields[i]
	}
	if result == nil {
		return currentFieldAuthority{}, fmt.Errorf("canonical field %q was not found", name)
	}
	return *result, nil
}

func (g *handlerGeneration) currentProjectionFields(generated *gen.Plan, expression string, cardinality spec.Cardinality) (currentFieldAuthorities, error) {
	base, err := g.recordBase(expression, cardinality)
	if err != nil {
		return nil, err
	}
	var result currentFieldAuthorities
	if view := generated.ViewByType(base); view != nil && view.Ownership == gen.ViewGenerated {
		for _, field := range view.Fields {
			canonical, err := generated.CanonicalType(g.input.TargetPackage, field.Type)
			if err != nil {
				return nil, err
			}
			result = append(result, currentFieldAuthority{name: field.Name, expression: canonical})
		}
		return result, nil
	}
	if g.input.TypeResolver == nil {
		return nil, fmt.Errorf("current projection type %s requires canonical type authority", base)
	}
	resolved, err := g.input.TypeResolver.ResolveShape(base)
	if err != nil {
		return nil, err
	}
	if resolved == nil || resolved.Descriptor == nil {
		return nil, fmt.Errorf("current projection type %s was not resolved", base)
	}
	fields, err := xshape.New(resolved.Descriptor, g.input.TypeResolver.Descriptor).Fields()
	if err != nil {
		return nil, err
	}
	for _, field := range fields {
		if !field.Exported {
			continue
		}
		canonical, err := field.CanonicalType()
		if err != nil {
			return nil, err
		}
		result = append(result, currentFieldAuthority{name: field.Name, expression: canonical})
	}
	return result, nil
}
