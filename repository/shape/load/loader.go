package load

import (
	"context"
	"fmt"
	"reflect"
	"strings"

	"github.com/viant/datly/repository/shape"
	dqlshape "github.com/viant/datly/repository/shape/dql/shape"
	"github.com/viant/datly/repository/shape/plan"
	"github.com/viant/datly/repository/shape/typectx"
	shapevalidate "github.com/viant/datly/repository/shape/validate"
	"github.com/viant/datly/shared"
	"github.com/viant/datly/view"
	"github.com/viant/datly/view/state"
)

// Loader materializes runtime view artifacts from normalized shape plan.
type Loader struct{}

// New returns shape loader implementation.
func New() *Loader {
	return &Loader{}
}

// LoadViews implements shape.Loader.
func (l *Loader) LoadViews(_ context.Context, planned *shape.PlanResult, _ ...shape.LoadOption) (*shape.ViewArtifacts, error) {
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
func (l *Loader) LoadComponent(_ context.Context, planned *shape.PlanResult, _ ...shape.LoadOption) (*shape.ComponentArtifact, error) {
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
	pResult, ok := planned.Plan.(*plan.Result)
	if !ok || pResult == nil {
		return nil, nil, fmt.Errorf("shape load: unsupported plan type %T", planned.Plan)
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
	return pResult, resource, nil
}

func buildComponent(source *shape.Source, pResult *plan.Result) *Component {
	ret := &Component{Method: "GET"}
	if source != nil {
		ret.Name = source.Name
		ret.URI = source.Name
	}
	for _, aView := range pResult.Views {
		if aView == nil {
			continue
		}
		ret.Views = append(ret.Views, aView.Name)
		if aView.Declaration != nil {
			if ret.Declarations == nil {
				ret.Declarations = map[string]*plan.ViewDeclaration{}
			}
			ret.Declarations[aView.Name] = aView.Declaration
			if selector := strings.TrimSpace(aView.Declaration.QuerySelector); selector != "" {
				if ret.QuerySelectors == nil {
					ret.QuerySelectors = map[string][]string{}
				}
				ret.QuerySelectors[selector] = append(ret.QuerySelectors[selector], aView.Name)
			}
			if len(aView.Declaration.Predicates) > 0 {
				if ret.Predicates == nil {
					ret.Predicates = map[string][]*plan.ViewPredicate{}
				}
				ret.Predicates[aView.Name] = append(ret.Predicates[aView.Name], aView.Declaration.Predicates...)
			}
		}
		if len(aView.Relations) > 0 {
			ret.Relations = append(ret.Relations, aView.Relations...)
			ret.ViewRelations = append(ret.ViewRelations, toViewRelations(aView.Relations)...)
		}
	}
	rootView := pickRootView(pResult.Views)
	if rootView != nil {
		ret.RootView = rootView.Name
		if ret.Name == "" {
			ret.Name = rootView.Name
		}
	}
	for _, item := range pResult.States {
		if item == nil {
			continue
		}
		kind := strings.ToLower(item.KindString())
		inName := item.InName()
		if kind == "" && inName == "" {
			ret.Other = append(ret.Other, item)
			continue
		}
		switch kind {
		case "query", "path", "header", "body", "form", "cookie", "request", "":
			ret.Input = append(ret.Input, item)
		case "output":
			ret.Output = append(ret.Output, item)
		case "meta":
			ret.Meta = append(ret.Meta, item)
		case "async":
			ret.Async = append(ret.Async, item)
		default:
			ret.Other = append(ret.Other, item)
		}
	}
	ret.TypeContext = cloneTypeContext(pResult.TypeContext)
	ret.Directives = cloneDirectives(pResult.Directives)
	ret.ColumnsDiscovery = pResult.ColumnsDiscovery
	return ret
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
