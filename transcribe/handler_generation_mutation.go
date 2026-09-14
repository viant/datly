package transcribe

import (
	"fmt"
	"github.com/viant/datly/spec"
	"github.com/viant/datly/tag"
	gen "github.com/viant/datly/transcribe/generate"
	plan "github.com/viant/datly/transcribe/handler/ast"
	"github.com/viant/datly/transcribe/handler/compiler"
	xshape "github.com/viant/x/shape"
	"strings"
)

// refineMutationFields resolves mutation-only selectors under the same final
// row authority as typed entity helpers, without changing authored root paths.
func (g *handlerGeneration) refineMutationFields(record *plan.RecordPlan, generated *gen.Plan, valueType string) error {
	if record.Auxiliary || (record.Sequence == nil && len(record.SelfRelations) == 0) {
		return nil
	}
	fields, err := g.currentProjectionFields(generated, valueType, record.Cardinality)
	if err != nil {
		return err
	}
	if record.Sequence != nil {
		field, err := fields.resolve(record.Sequence.Field.Field)
		if err != nil {
			return fmt.Errorf("sequence field for %s: %w", record.Identity, err)
		}
		record.Sequence.Field.Field, record.Sequence.Field.Type = field.name, spec.TypeRef{Name: field.expression}
	}
	for index, relation := range record.SelfRelations {
		holder, err := fields.resolve(strings.Join(relation.FieldPath, "."))
		if err != nil {
			return fmt.Errorf("self holder for %s: %w", record.Identity, err)
		}
		relation.FieldPath = plan.FieldPath{holder.name}
		relation.Links, err = g.refineMutationLinks(relation.Links, fields, fields)
		if err != nil {
			return err
		}
		record.SelfRelations[index] = relation
	}
	return nil
}

func (g *handlerGeneration) refineMutationLinks(links []plan.KeyLink, parent, child currentFieldAuthorities) ([]plan.KeyLink, error) {
	result := append([]plan.KeyLink(nil), links...)
	for index, link := range links {
		from, err := parent.resolve(link.Parent.Field)
		if err != nil {
			return nil, err
		}
		to, err := child.resolve(link.Child.Field)
		if err != nil {
			return nil, err
		}
		projection, err := (&compiler.Compiler{}).RefineCurrentField(plan.CurrentField{Current: plan.FieldRef{Field: from.name}, Entity: plan.FieldRef{Field: to.name}}, from.expression, to.expression)
		if err != nil {
			return nil, fmt.Errorf("mutation link %s to %s: %w", from.name, to.name, err)
		}
		link.Parent.Field, link.Parent.Type = from.name, projection.Current.Type
		link.Child.Field, link.Child.Type = to.name, projection.Entity.Type
		link.Conversion = projection.Conversion
		result[index] = link
	}
	return result, nil
}

func (g *handlerGeneration) refineRelationMutationFields(relation *plan.RelationPlan, parent *plan.RecordPlan, generated *gen.Plan, parentType, childType string) error {
	if parent.Auxiliary || relation.Child.Auxiliary {
		return nil
	}
	parentFields, err := g.currentProjectionFields(generated, parentType, parent.Cardinality)
	if err != nil {
		return err
	}
	childFields, err := g.currentProjectionFields(generated, childType, relation.Child.Cardinality)
	if err != nil {
		return err
	}
	links, err := g.refineMutationLinks(relation.Links, parentFields, childFields)
	if err != nil {
		return err
	}
	relation.Links = links
	return nil
}

func (g *handlerGeneration) refineLinkedSelfRelations(record *plan.RecordPlan, fields []xshape.Field) error {
	var authority currentFieldAuthorities
	for _, field := range fields {
		if !field.Exported {
			continue
		}
		expression, err := field.CanonicalType()
		if err != nil {
			return err
		}
		authority = append(authority, currentFieldAuthority{name: field.Name, expression: expression})
	}
	result := append([]plan.SelfRelationPlan(nil), record.SelfRelations...)
	for _, field := range fields {
		if !field.Exported {
			continue
		}
		raw, ok := field.Tag.Lookup(tag.SelfName)
		if !ok {
			continue
		}
		self, err := tag.ParseSelf(raw)
		if err != nil {
			return err
		}
		if self == nil {
			continue
		}
		from, err := authority.resolve(self.Child)
		if err != nil {
			return err
		}
		to, err := authority.resolve(self.Parent)
		if err != nil {
			return err
		}
		candidate := plan.KeyLink{Parent: plan.KeyPart{Field: from.name, Source: self.Child}, Child: plan.KeyPart{Field: to.name, Source: self.Parent}}
		links, err := g.refineMutationLinks([]plan.KeyLink{candidate}, authority, authority)
		if err != nil {
			return err
		}
		found := false
		for _, prior := range result {
			holder, err := authority.resolve(strings.Join(prior.FieldPath, "."))
			if err != nil {
				return err
			}
			if holder.name != field.Name {
				continue
			}
			existing, err := g.refineMutationLinks(prior.Links, authority, authority)
			if err != nil {
				return err
			}
			if len(existing) != 1 || existing[0].Parent.Field != from.name || existing[0].Child.Field != to.name {
				return fmt.Errorf("entity self holder %s conflicts with compiled link authority", field.Name)
			}
			found = true
		}
		if !found {
			result = append(result, plan.SelfRelationPlan{FieldPath: plan.FieldPath{field.Name}, Links: links})
		}
	}
	record.SelfRelations = result
	return nil
}
