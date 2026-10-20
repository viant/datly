package handlergen

import (
	"fmt"
	"reflect"

	"github.com/viant/datly/spec"
	"github.com/viant/datly/tag"
	gen "github.com/viant/datly/transcribe/generate"
	plan "github.com/viant/datly/transcribe/handler/ast"
	handlergo "github.com/viant/datly/transcribe/handler/golang"
	xshape "github.com/viant/x/shape"
)

// entitySetters keeps an auxiliary canonical Input lifecycle's public row
// behavior independent of physical entity execution. Marker selection and SQL
// ownership were already resolved by the final contract plan.
func (g *Generation) entitySetters(semantic *plan.Plan, config handlergo.Config, contracts *gen.Plan) (*handlergo.EntityAsset, error) {
	root := semantic.Root
	if !root.Auxiliary || root.Entity == nil || !root.Entity.HooksComponent {
		return handlergo.EntitySupport(semantic, config)
	}
	var owner *gen.ViewPlan
	named := map[string]*gen.ViewPlan{}
	for i := range contracts.Views {
		view := &contracts.Views[i]
		if view.Ownership != gen.ViewGenerated {
			continue
		}
		pkg := view.Package
		if pkg == "" {
			pkg = contracts.Package
		}
		if pkg != config.PackagePath {
			continue
		}
		canonical, err := contracts.CanonicalType(pkg, view.Type)
		if err != nil {
			return nil, err
		}
		named[canonical] = view
		if view.Identity == root.Identity {
			owner = view
		}
	}
	if owner == nil || len(owner.SetMarkerFields) == 0 {
		return nil, nil
	}
	var records []handlergo.AccessorRecord
	visited := map[*gen.ViewPlan]bool{}
	var visit func(*gen.ViewPlan) error
	visit = func(view *gen.ViewPlan) error {
		if visited[view] {
			return nil
		}
		visited[view] = true
		entity := &plan.EntityPlan{Owned: true, MarkerField: "Has", MarkerPointer: true, MarkerType: spec.TypeRef{Name: view.Name + "Has"}}
		for _, name := range view.SetMarkerFields {
			field, ok := view.Field(name)
			if !ok {
				return fmt.Errorf("entity %s marker field %s is missing", view.Name, name)
			}
			planned := plan.EntityField{Name: name, Path: plan.FieldPath{name}, Type: spec.TypeRef{Name: field.Type}, Writable: true}
			if _, relation := reflect.StructTag(field.Tag).Lookup(tag.ViewName); relation {
				// This accessor-only record preserves marked relation setters;
				// it does not grant the auxiliary relation persistence ownership.
				planned.Relation = true
			}
			if _, self := reflect.StructTag(field.Tag).Lookup(tag.SelfName); self {
				planned.Relation, planned.Self, planned.Writable = true, true, true
			}
			entity.Fields = append(entity.Fields, planned)
		}
		if len(entity.Fields) != 0 {
			records = append(records, handlergo.AccessorRecord{Type: view.Type, Entity: entity})
		}
		// Follow exactly the final locally generated named row dependencies.
		// Linked owners and independent reads outside this closure keep their
		// existing method ownership; marker structs are never row receivers.
		for _, field := range view.Fields {
			canonical, err := contracts.CanonicalType(contracts.Package, field.Type)
			if err != nil {
				return err
			}
			_, err = (xshape.Resolver{Rewriter: func(key string) (string, error) {
				if dependency := named[key]; dependency != nil {
					if err := visit(dependency); err != nil {
						return "", err
					}
				}
				return key, nil
			}}).Rewrite(canonical)
			if err != nil {
				return err
			}
		}
		return nil
	}
	if err := visit(owner); err != nil {
		return nil, err
	}
	return handlergo.EntityAccessors(records, config)
}
