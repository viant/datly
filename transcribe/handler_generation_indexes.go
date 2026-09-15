package transcribe

import (
	"fmt"

	"github.com/viant/datly/spec"
	gen "github.com/viant/datly/transcribe/generate"
	plan "github.com/viant/datly/transcribe/handler/ast"
	"github.com/viant/datly/typecatalog"
)

// Application collections include independent auxiliary reads. They share
// canonical view/contract authority, not the mutation traversal graph.
func (g *handlerGeneration) readCollections(semantic *plan.Plan, generated *gen.Plan) error {
	for _, parameter := range g.compiled.Component.Parameters {
		if parameter == nil || parameter.Source.Kind != "view" {
			continue
		}
		identity := g.compiled.ViewBindings[parameter.Identity()]
		var view *spec.View
		for _, candidate := range g.compiled.Component.Views {
			if candidate == nil {
				continue
			}
			actual, err := candidate.Identity()
			if err != nil {
				return err
			}
			if actual == identity {
				view = candidate
				break
			}
		}
		if view == nil {
			return fmt.Errorf("application read %s has no canonical view", parameter.Name)
		}
		path := plan.FieldPath{"Input", typecatalog.FieldName(parameter.Name)}
		current := &plan.CurrentPlan{ViewIdentity: identity, InputPath: path}
		rows, _, err := g.currentRecordTypes(generated, current)
		if err != nil {
			return err
		}
		base, err := g.recordBase(rows, spec.CardinalityMany)
		if err != nil {
			return err
		}
		fields, err := g.currentProjectionFields(generated, rows, spec.CardinalityMany)
		if err != nil {
			return err
		}
		canonical, err := generated.CanonicalType(g.input.TargetPackage, base)
		if err != nil {
			return err
		}
		read := plan.ReadCollection{Name: path[1], InputPath: path, Type: spec.TypeRef{Name: canonical}}
		for _, column := range view.Columns {
			if column == nil {
				continue
			}
			field, err := fields.resolve(column.Name)
			if err != nil {
				return err
			}
			read.Fields = append(read.Fields, plan.FieldRef{Field: field.name, Source: column.Source, Type: spec.TypeRef{Name: field.expression}})
			if column.PrimaryKey {
				read.Keys = append(read.Keys, plan.KeyPart{Field: field.name, Source: column.Source, Type: spec.TypeRef{Name: field.expression}})
			}
		}
		semantic.ReadCollections = append(semantic.ReadCollections, read)
	}
	return (&readGroupCompilation{reads: semantic.ReadCollections}).compile(semantic.Root)
}
