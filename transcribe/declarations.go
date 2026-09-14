package transcribe

import (
	"fmt"
	"strings"

	"github.com/viant/datly/spec"
	tcompile "github.com/viant/datly/transcribe/compile"
	"github.com/viant/datly/transcribe/dql"
	gen "github.com/viant/datly/transcribe/generate"
	"github.com/viant/tagly/tags"
)

const diagnosticDeclarationSQL = "DQL-DECL-SQL"

type declarationError struct {
	param    string
	identity string
	err      error
}

func (e *declarationError) Error() string {
	return fmt.Sprintf("analyze declaration SQL for parameter %s: %v", e.param, e.err)
}

func (e *declarationError) Unwrap() error { return e.err }

type declarationProducts struct {
	generation         gen.Declarations
	views              []*spec.View
	viewsByParam       map[string]*spec.View
	outputRelations    []*spec.Relation
	skeletonIdentities map[string]bool
}

type declarationCompiler struct {
	component     *spec.Component
	authoredViews []*spec.View
	reader        *tcompile.Reader
}

func newDeclarationCompiler(component *spec.Component, authoredViews []*spec.View) *declarationCompiler {
	return &declarationCompiler{component: component, authoredViews: authoredViews, reader: tcompile.NewReader()}
}

func (c *declarationCompiler) compile() (*declarationProducts, error) {
	if c == nil || c.component == nil {
		return nil, nil
	}
	result := &declarationProducts{}
	viewNames := map[string]bool{}
	if c.component.RootView != nil {
		viewNames[strings.TrimSpace(c.component.RootView.Name)] = true
	}
	for _, param := range spec.EffectiveParameters(c.component.Parameters) {
		if param == nil || strings.TrimSpace(param.DeclarationSQL) == "" {
			continue
		}
		analysis, err := dql.AnalyzeDeclarationSQL(param.DeclarationSQL)
		if err != nil {
			return nil, &declarationError{param: strings.TrimSpace(param.Name), identity: param.Identity(), err: err}
		}
		if analysis == nil {
			continue
		}
		if c.requiresGeneratedHelperProjection(param, analysis) && (!analysis.ProjectionComplete || len(analysis.Projection) == 0) {
			return nil, &declarationError{
				param: param.Name, identity: param.Identity(),
				err: fmt.Errorf("generated StructQL helper requires an explicit type or a projection of fields, selectors, or one ARRAY_AGG field"),
			}
		}
		if result.generation == nil {
			result.generation = gen.Declarations{}
		}
		result.generation[param.Identity()] = gen.Declaration{
			Projection:       c.generationProjection(analysis.Projection),
			NestedChildField: analysis.NestedChildField(),
			DataType:         analysis.DataType,
		}
		if param.IsDerivedOutput() {
			if c.component.RootView == nil {
				return nil, &declarationError{param: param.Name, identity: param.Identity(), err: fmt.Errorf("output relation requires a root view")}
			}
			view := &spec.View{
				Key: spec.Key{Kind: spec.KindView, Scope: c.component.Key.Scope, Name: param.Name}, Name: param.Name,
				Cardinality: spec.CardinalityOne,
				Source:      &spec.ViewSource{SQL: analysis.SQL, URI: strings.TrimSpace(param.ResourceRef), Embeds: dql.EmbeddedSQLRefs(analysis.SQL)},
			}
			compiled, err := c.reader.Compile(tcompile.ReadInput{View: view, SQL: analysis.SQL})
			if err != nil {
				return nil, &declarationError{param: param.Name, identity: param.Identity(), err: err}
			}
			result.outputRelations = append(result.outputRelations, &spec.Relation{
				Name: param.Name, Kind: spec.RelationKindDerived, Holder: param.Name,
				Cardinality: spec.CardinalityOne, View: compiled,
			})
			continue
		}
		if !strings.EqualFold(strings.TrimSpace(param.Source.Kind), "view") {
			continue
		}
		name := strings.TrimSpace(param.Name)
		if name == "" {
			return nil, &declarationError{param: name, identity: param.Identity(), err: fmt.Errorf("view name is required")}
		}
		if viewNames[name] {
			return nil, &declarationError{param: name, identity: param.Identity(), err: fmt.Errorf("view %q is declared more than once", name)}
		}
		view, skeletonIdentity, err := c.canonicalView(param)
		if err != nil {
			return nil, &declarationError{param: name, identity: param.Identity(), err: err}
		}
		if skeletonIdentity != "" {
			if result.skeletonIdentities == nil {
				result.skeletonIdentities = map[string]bool{}
			}
			result.skeletonIdentities[skeletonIdentity] = true
		}
		if view == nil {
			view = &spec.View{Key: spec.Key{Kind: spec.KindView, Scope: c.component.Key.Scope, Name: name}, Name: name}
		}
		view.Source = view.Source.Clone()
		if view.Source == nil {
			view.Source = &spec.ViewSource{}
		}
		view.Source.SQL = analysis.SQL
		view.Source.URI = strings.TrimSpace(param.ResourceRef)
		view.Source.Embeds = dql.EmbeddedSQLRefs(analysis.SQL)
		compiled, err := c.reader.Compile(tcompile.ReadInput{View: view, SQL: analysis.SQL})
		if err != nil {
			return nil, &declarationError{param: name, identity: param.Identity(), err: err}
		}
		viewNames[name] = true
		result.views = append(result.views, compiled)
		if result.viewsByParam == nil {
			result.viewsByParam = map[string]*spec.View{}
		}
		result.viewsByParam[param.Identity()] = compiled
	}
	return result, nil
}

func (c *declarationCompiler) requiresGeneratedHelperProjection(param *spec.Parameter, analysis *dql.DeclarationAnalysis) bool {
	if param == nil || analysis == nil || !strings.EqualFold(strings.TrimSpace(param.Source.Kind), "param") ||
		strings.TrimSpace(param.TypeExpr) != "" || strings.TrimSpace(param.OutputTypeExpr) != "" || strings.TrimSpace(analysis.DataType) != "" {
		return false
	}
	return tags.NewTags(strings.TrimSpace(param.Tag)).Lookup("typeName") == nil
}

func (c *declarationCompiler) generationProjection(source []dql.DeclarationProjection) []gen.DeclarationProjection {
	if len(source) == 0 {
		return nil
	}
	result := make([]gen.DeclarationProjection, 0, len(source))
	for _, field := range source {
		result = append(result, gen.DeclarationProjection{
			Name: field.Name, Source: field.Source, Aggregate: field.Aggregate,
		})
	}
	return result
}

func (c *declarationCompiler) canonicalView(param *spec.Parameter) (*spec.View, string, error) {
	if param == nil {
		return nil, "", nil
	}
	name := strings.TrimSpace(param.Name)
	expected := &spec.View{Key: spec.Key{Kind: spec.KindView, Scope: strings.TrimSpace(c.component.Key.Scope), Name: name}, Name: name}
	expectedIdentity, err := expected.Identity()
	if err != nil {
		return nil, "", err
	}
	var result *spec.View
	for _, view := range c.authoredViews {
		if view == nil {
			continue
		}
		identity, err := view.Identity()
		if err != nil {
			return nil, "", err
		}
		if identity != expectedIdentity {
			continue
		}
		if result != nil {
			return nil, "", fmt.Errorf("view %q has ambiguous declaration metadata", name)
		}
		result = view
	}
	if result == nil {
		return nil, "", nil
	}
	if result.Source == nil || strings.TrimSpace(result.Source.SQL) != strings.TrimSpace(param.DeclarationSQL) {
		return nil, "", fmt.Errorf("view %q declaration metadata does not match its SQL", name)
	}
	return result.Clone(), expectedIdentity, nil
}

func (p *declarationProducts) removeSkeletons(views []*spec.View) ([]*spec.View, error) {
	if p == nil || len(views) == 0 || len(p.skeletonIdentities) == 0 {
		return views, nil
	}
	result := make([]*spec.View, 0, len(views))
	for _, view := range views {
		if view == nil {
			result = append(result, view)
			continue
		}
		identity, err := view.Identity()
		if err != nil {
			return nil, err
		}
		if !p.skeletonIdentities[identity] {
			result = append(result, view)
		}
	}
	return result, nil
}
