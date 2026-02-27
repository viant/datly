package load

import (
	"context"
	"fmt"
	"reflect"
	"strings"
	"time"

	"github.com/viant/datly/repository/shape"
	dqlshape "github.com/viant/datly/repository/shape/dql/shape"
	"github.com/viant/datly/repository/shape/plan"
	"github.com/viant/datly/repository/shape/typectx"
	shapevalidate "github.com/viant/datly/repository/shape/validate"
	"github.com/viant/datly/shared"
	"github.com/viant/datly/view"
	"github.com/viant/datly/view/extension"
	"github.com/viant/datly/view/state"
)

// Loader materializes runtime view artifacts from normalized shape plan.
type Loader struct{}

// New returns shape loader implementation.
func New() *Loader {
	return &Loader{}
}

// LoadViews implements shape.Loader.
func (l *Loader) LoadViews(ctx context.Context, planned *shape.PlanResult, _ ...shape.LoadOption) (*shape.ViewArtifacts, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	pResult, resource, err := l.materialize(planned)
	if err != nil {
		return nil, err
	}
	if len(pResult.Views) == 0 {
		return nil, ErrEmptyViewPlan
	}
	return &shape.ViewArtifacts{Resource: resource, Views: resource.Views}, nil
}

// LoadComponent implements shape.Loader.
func (l *Loader) LoadComponent(ctx context.Context, planned *shape.PlanResult, _ ...shape.LoadOption) (*shape.ComponentArtifact, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	pResult, resource, err := l.materialize(planned)
	if err != nil {
		return nil, err
	}
	if len(pResult.Views) == 0 {
		return nil, ErrEmptyViewPlan
	}
	component := buildComponent(planned.Source, pResult)
	return &shape.ComponentArtifact{
		Resource:  resource,
		Component: component,
	}, nil
}

func (l *Loader) materialize(planned *shape.PlanResult) (*plan.Result, *view.Resource, error) {
	if planned == nil || planned.Source == nil {
		return nil, nil, shape.ErrNilSource
	}
	pResult, ok := plan.ResultFrom(planned)
	if !ok {
		return nil, nil, fmt.Errorf("shape load: unsupported plan kind %q", planned.Plan.ShapeSpecKind())
	}
	resource := view.EmptyResource()
	if pResult.EmbedFS != nil {
		resource.SetFSEmbedder(state.NewFSEmbedder(pResult.EmbedFS))
	}
	for _, item := range pResult.Views {
		aView, err := materializeView(item)
		if err != nil {
			return nil, nil, err
		}
		resource.AddViews(aView)
	}
	if err := shapevalidate.ValidateRelations(resource, resource.Views...); err != nil {
		return nil, nil, err
	}
	// Gap 7: apply global cache TTL directive to root view.
	if pResult.Directives != nil && pResult.Directives.Cache != nil {
		if ttl := strings.TrimSpace(pResult.Directives.Cache.TTL); ttl != "" {
			if dur, err := time.ParseDuration(ttl); err == nil && dur > 0 {
				ttlMs := int(dur.Milliseconds())
				if rootPlan := pickRootView(pResult.Views); rootPlan != nil {
					for _, rv := range resource.Views {
						if rv != nil && rv.Name == rootPlan.Name {
							if rv.Cache == nil {
								rv.Cache = &view.Cache{}
							}
							rv.Cache.TimeToLiveMs = ttlMs
							break
						}
					}
				}
			}
		}
	}
	return pResult, resource, nil
}

func buildComponent(source *shape.Source, pResult *plan.Result) *Component {
	component := &Component{Method: "GET"}
	if source != nil {
		component.Name = source.Name
		component.URI = source.Name
	}
	applyViewMeta(component, pResult.Views)
	applyStateBuckets(component, pResult.States)
	component.Input = append(component.Input, synthesizePredicateStates(component.Input, component.Predicates)...)
	component.TypeContext = cloneTypeContext(pResult.TypeContext)
	component.Directives = cloneDirectives(pResult.Directives)
	component.ColumnsDiscovery = pResult.ColumnsDiscovery
	return component
}

// applyViewMeta populates the component with view names, declarations, relations,
// query selectors, predicate maps, and root view from the plan view list.
func applyViewMeta(component *Component, views []*plan.View) {
	for _, aView := range views {
		if aView == nil {
			continue
		}
		component.Views = append(component.Views, aView.Name)
		if aView.Declaration != nil {
			indexViewDeclaration(component, aView.Name, aView.Declaration)
		}
		if len(aView.Relations) > 0 {
			component.Relations = append(component.Relations, aView.Relations...)
			component.ViewRelations = append(component.ViewRelations, toViewRelations(aView.Relations)...)
		}
	}
	if rootView := pickRootView(views); rootView != nil {
		component.RootView = rootView.Name
		if component.Name == "" {
			component.Name = rootView.Name
		}
	}
}

// indexViewDeclaration registers the declaration's query selector and predicates
// on the component index maps, creating them on demand.
func indexViewDeclaration(component *Component, viewName string, decl *plan.ViewDeclaration) {
	if component.Declarations == nil {
		component.Declarations = map[string]*plan.ViewDeclaration{}
	}
	component.Declarations[viewName] = decl
	if selector := strings.TrimSpace(decl.QuerySelector); selector != "" {
		if component.QuerySelectors == nil {
			component.QuerySelectors = map[string][]string{}
		}
		component.QuerySelectors[selector] = append(component.QuerySelectors[selector], viewName)
	}
	if len(decl.Predicates) > 0 {
		if component.Predicates == nil {
			component.Predicates = map[string][]*plan.ViewPredicate{}
		}
		component.Predicates[viewName] = append(component.Predicates[viewName], decl.Predicates...)
	}
}

// applyStateBuckets sorts plan states into the typed buckets on the component
// (Input, Output, Meta, Async, Other) based on the state's location kind.
func applyStateBuckets(component *Component, states []*plan.State) {
	for _, item := range states {
		if item == nil {
			continue
		}
		kind := state.Kind(strings.ToLower(item.KindString()))
		inName := item.InName()
		if kind == "" && inName == "" {
			component.Other = append(component.Other, item)
			continue
		}
		switch kind {
		case state.KindQuery, state.KindPath, state.KindHeader, state.KindRequestBody,
			state.KindForm, state.KindCookie, state.KindRequest, "":
			component.Input = append(component.Input, item)
		case state.KindOutput:
			component.Output = append(component.Output, item)
		case state.KindMeta:
			component.Meta = append(component.Meta, item)
		case state.KindAsync:
			component.Async = append(component.Async, item)
		default:
			component.Other = append(component.Other, item)
		}
	}
}

// synthesizePredicateStates creates query parameters for view-level predicates whose
// source parameter is not already present in the input state list.
func synthesizePredicateStates(input []*plan.State, predicates map[string][]*plan.ViewPredicate) []*plan.State {
	if len(predicates) == 0 {
		return nil
	}
	declared := make(map[string]bool, len(input))
	for _, s := range input {
		if s != nil {
			declared[strings.ToLower(strings.TrimPrefix(strings.TrimSpace(s.Name), "$"))] = true
		}
	}
	var result []*plan.State
	for _, viewPredicates := range predicates {
		for _, vp := range viewPredicates {
			if vp == nil {
				continue
			}
			src := strings.TrimPrefix(strings.TrimSpace(vp.Source), "$")
			if src == "" || declared[strings.ToLower(src)] {
				continue
			}
			result = append(result, &plan.State{
				Parameter: state.Parameter{
					Name:   src,
					In:     state.NewQueryLocation(src),
					Schema: &state.Schema{DataType: "string"},
					Predicates: []*extension.PredicateConfig{
						{
							Name:   vp.Name,
							Ensure: vp.Ensure,
							Args:   append([]string{}, vp.Arguments...),
						},
					},
				},
			})
			declared[strings.ToLower(src)] = true
		}
	}
	return result
}

func cloneTypeContext(input *typectx.Context) *typectx.Context {
	if input == nil {
		return nil
	}
	ret := &typectx.Context{
		DefaultPackage: strings.TrimSpace(input.DefaultPackage),
		PackageDir:     strings.TrimSpace(input.PackageDir),
		PackageName:    strings.TrimSpace(input.PackageName),
		PackagePath:    strings.TrimSpace(input.PackagePath),
	}
	for _, item := range input.Imports {
		pkg := strings.TrimSpace(item.Package)
		if pkg == "" {
			continue
		}
		ret.Imports = append(ret.Imports, typectx.Import{
			Alias:   strings.TrimSpace(item.Alias),
			Package: pkg,
		})
	}
	if ret.DefaultPackage == "" &&
		len(ret.Imports) == 0 &&
		ret.PackageDir == "" &&
		ret.PackageName == "" &&
		ret.PackagePath == "" {
		return nil
	}
	return ret
}

func cloneDirectives(input *dqlshape.Directives) *dqlshape.Directives {
	if input == nil {
		return nil
	}
	ret := &dqlshape.Directives{
		Meta:             strings.TrimSpace(input.Meta),
		DefaultConnector: strings.TrimSpace(input.DefaultConnector),
	}
	if input.Cache != nil {
		ret.Cache = &dqlshape.CacheDirective{
			Enabled: input.Cache.Enabled,
			TTL:     strings.TrimSpace(input.Cache.TTL),
		}
	}
	if input.MCP != nil {
		ret.MCP = &dqlshape.MCPDirective{
			Name:            strings.TrimSpace(input.MCP.Name),
			Description:     strings.TrimSpace(input.MCP.Description),
			DescriptionPath: strings.TrimSpace(input.MCP.DescriptionPath),
		}
	}
	if ret.Meta == "" && ret.DefaultConnector == "" && ret.Cache == nil && ret.MCP == nil {
		return nil
	}
	return ret
}

func pickRootView(views []*plan.View) *plan.View {
	var selected *plan.View
	minDepth := -1
	for _, candidate := range views {
		if candidate == nil || candidate.Path == "" {
			continue
		}
		depth := strings.Count(candidate.Path, ".")
		if minDepth == -1 || depth < minDepth {
			minDepth = depth
			selected = candidate
		}
	}
	if selected != nil {
		return selected
	}
	for _, candidate := range views {
		if candidate != nil {
			return candidate
		}
	}
	return nil
}

func materializeView(item *plan.View) (*view.View, error) {
	if item == nil {
		return nil, fmt.Errorf("shape load: nil view plan item")
	}

	schemaType := bestSchemaType(item)
	if schemaType == nil {
		return nil, fmt.Errorf("shape load: missing schema type for view %q", item.Name)
	}

	schema := newSchema(schemaType, item.Cardinality)
	mode := view.ModeQuery
	switch strings.TrimSpace(item.Mode) {
	case string(view.ModeExec):
		mode = view.ModeExec
	case string(view.ModeHandler):
		mode = view.ModeHandler
	case string(view.ModeQuery):
		mode = view.ModeQuery
	}
	opts := []view.Option{view.WithSchema(schema), view.WithMode(mode)}

	if item.Connector != "" {
		opts = append(opts, view.WithConnectorRef(item.Connector))
	}
	if item.SQL != "" || item.SQLURI != "" {
		tmpl := view.NewTemplate(item.SQL)
		tmpl.SourceURL = item.SQLURI
		if strings.TrimSpace(item.Summary) != "" {
			tmpl.Summary = &view.TemplateSummary{
				Name:   "Summary",
				Source: item.Summary,
				Kind:   view.MetaKindRecord,
			}
		}
		opts = append(opts, view.WithTemplate(tmpl))
	}
	if item.CacheRef != "" {
		opts = append(opts, view.WithCache(&view.Cache{Reference: shared.Reference{Ref: item.CacheRef}}))
	}
	if item.Partitioner != "" {
		opts = append(opts, view.WithPartitioned(&view.Partitioned{
			DataType:    item.Partitioner,
			Concurrency: item.PartitionedConcurrency,
		}))
	}

	aView, err := view.New(item.Name, item.Table, opts...)
	if err != nil {
		return nil, err
	}
	aView.Ref = item.Ref
	aView.Module = item.Module
	aView.AllowNulls = item.AllowNulls
	// Gap 6: forward view-level tag from declaration.
	if item.Declaration != nil && strings.TrimSpace(item.Declaration.Tag) != "" {
		aView.Tag = strings.TrimSpace(item.Declaration.Tag)
	}
	if strings.TrimSpace(item.SelectorNamespace) != "" || item.SelectorNoLimit != nil {
		if aView.Selector == nil {
			aView.Selector = &view.Config{}
		}
		if strings.TrimSpace(item.SelectorNamespace) != "" {
			aView.Selector.Namespace = strings.TrimSpace(item.SelectorNamespace)
		}
		if item.SelectorNoLimit != nil {
			aView.Selector.NoLimit = *item.SelectorNoLimit
		}
	}
	if aView.Schema != nil && strings.TrimSpace(item.SchemaType) != "" {
		if aView.Schema.DataType == "" {
			aView.Schema.DataType = strings.TrimSpace(item.SchemaType)
		}
		if aView.Schema.Name == "" {
			aView.Schema.Name = strings.Trim(strings.TrimSpace(item.SchemaType), "*")
		}
	}
	// Populate columns from statically-inferred struct type so that xgen can
	// generate accurate Go struct definitions during bootstrap. Only applied when
	// the view has no columns yet (avoids overwriting explicit column config).
	if len(aView.Columns) == 0 {
		if cols := inferColumnsFromType(item.ElementType); len(cols) > 0 {
			aView.Columns = cols
		}
	}
	return aView, nil
}

func bestSchemaType(item *plan.View) reflect.Type {
	if item.FieldType != nil {
		return item.FieldType
	}
	if item.ElementType != nil {
		return item.ElementType
	}
	return nil
}

func toViewRelations(input []*plan.Relation) []*view.Relation {
	if len(input) == 0 {
		return nil
	}
	result := make([]*view.Relation, 0, len(input))
	for _, item := range input {
		if item == nil {
			continue
		}
		relation := &view.Relation{
			Name:   item.Name,
			Holder: item.Holder,
			On:     toViewLinks(item.On, true),
			Of: view.NewReferenceView(
				toViewLinks(item.On, false),
				view.NewView(item.Ref, item.Table),
			),
		}
		result = append(result, relation)
	}
	return result
}

func toViewLinks(input []*plan.RelationLink, parent bool) view.Links {
	if len(input) == 0 {
		return nil
	}
	result := make(view.Links, 0, len(input))
	for _, item := range input {
		if item == nil {
			continue
		}
		link := &view.Link{}
		if parent {
			link.Field = item.ParentField
			link.Namespace = item.ParentNamespace
			link.Column = item.ParentColumn
		} else {
			link.Field = item.RefField
			link.Namespace = item.RefNamespace
			link.Column = item.RefColumn
		}
		result = append(result, link)
	}
	return result
}

func newSchema(rType reflect.Type, cardinality string) *state.Schema {
	if cardinality == "many" && rType.Kind() != reflect.Slice {
		return state.NewSchema(rType, state.WithMany())
	}
	return state.NewSchema(rType)
}
