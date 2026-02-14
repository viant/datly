package load

import (
	"context"
	"fmt"
	"reflect"
	"strings"

	"github.com/viant/datly/repository/shape"
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
		if strings.TrimSpace(item.Kind) == "" && strings.TrimSpace(item.In) == "" {
			ret.Other = append(ret.Other, item)
			continue
		}
		switch strings.ToLower(item.Kind) {
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
	return ret
}

func cloneTypeContext(input *typectx.Context) *typectx.Context {
	if input == nil {
		return nil
	}
	ret := &typectx.Context{
		DefaultPackage: strings.TrimSpace(input.DefaultPackage),
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
	if ret.DefaultPackage == "" && len(ret.Imports) == 0 {
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
	opts := []view.Option{view.WithSchema(schema), view.WithMode(view.ModeQuery)}

	if item.Connector != "" {
		opts = append(opts, view.WithConnectorRef(item.Connector))
	}
	if item.SQL != "" || item.SQLURI != "" {
		tmpl := view.NewTemplate(item.SQL)
		tmpl.SourceURL = item.SQLURI
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

func newSchema(rType reflect.Type, cardinality string) *state.Schema {
	if cardinality == "many" && rType.Kind() != reflect.Slice {
		return state.NewSchema(rType, state.WithMany())
	}
	return state.NewSchema(rType)
}
