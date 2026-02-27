package load

import (
	"context"
	"embed"
	"reflect"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/viant/datly/repository/shape"
	dqlshape "github.com/viant/datly/repository/shape/dql/shape"
	"github.com/viant/datly/repository/shape/plan"
	"github.com/viant/datly/repository/shape/scan"
	"github.com/viant/datly/repository/shape/typectx"
)

//go:embed testdata/*.sql
var testFS embed.FS

type embeddedFS struct{}

func (embeddedFS) EmbedFS() *embed.FS {
	return &testFS
}

type reportRow struct {
	ID   int
	Name string
}

type reportSource struct {
	embeddedFS
	Rows   []reportRow `view:"rows,table=REPORT,connector=dev,cache=c1" sql:"uri=testdata/report.sql"`
	ID     int         `parameter:"id,kind=query,in=id"`
	Status any         `parameter:"status,kind=output,in=status"`
	Job    any         `parameter:"job,kind=async,in=job"`
	Meta   any         `parameter:"meta,kind=meta,in=view.name"`
}

func TestLoader_LoadViews(t *testing.T) {
	scanner := scan.New()
	scanned, err := scanner.Scan(context.Background(), &shape.Source{Struct: &reportSource{}})
	require.NoError(t, err)

	planner := plan.New()
	planned, err := planner.Plan(context.Background(), scanned)
	require.NoError(t, err)

	loader := New()
	artifacts, err := loader.LoadViews(context.Background(), planned)
	require.NoError(t, err)
	require.NotNil(t, artifacts)
	require.NotNil(t, artifacts.Resource)
	require.Len(t, artifacts.Views, 1)

	aView := artifacts.Views[0]
	assert.Equal(t, "rows", aView.Name)
	assert.Equal(t, "REPORT", aView.Table)
	require.NotNil(t, aView.Schema)
	assert.Equal(t, "Many", string(aView.Schema.Cardinality))
	require.NotNil(t, aView.Template)
	assert.Equal(t, "testdata/report.sql", aView.Template.SourceURL)
	assert.Contains(t, aView.Template.Source, "SELECT ID, NAME FROM REPORT")
	require.NotNil(t, aView.Connector)
	assert.Equal(t, "dev", aView.Connector.Ref)
	require.NotNil(t, aView.Cache)
	assert.Equal(t, "c1", aView.Cache.Ref)
	require.NotNil(t, artifacts.Resource.EmbedFS())
}

// stubPlanSpec is a non-plan-Result implementation of shape.PlanSpec used to
// verify that LoadViews() returns an error when given an unexpected plan type.
type stubPlanSpec struct{}

func (s *stubPlanSpec) ShapeSpecKind() string { return "stub" }

func TestLoader_LoadViews_InvalidPlanType(t *testing.T) {
	loader := New()
	_, err := loader.LoadViews(context.Background(), &shape.PlanResult{Source: &shape.Source{Name: "x"}, Plan: &stubPlanSpec{}})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "unsupported plan kind")
}

func TestLoader_LoadViews_Metadata(t *testing.T) {
	noLimit := true
	allowNulls := true
	planned := &shape.PlanResult{
		Source: &shape.Source{Name: "meta"},
		Plan: &plan.Result{
			Views: []*plan.View{
				{
					Name:              "items",
					Table:             "ITEMS",
					Module:            "platform/items",
					AllowNulls:        &allowNulls,
					SelectorNamespace: "it",
					SelectorNoLimit:   &noLimit,
					SchemaType:        "*ItemView",
					Cardinality:       "many",
					FieldType:         reflect.TypeOf([]map[string]interface{}{}),
					ElementType:       reflect.TypeOf(map[string]interface{}{}),
					SQL:               "SELECT * FROM ITEMS",
				},
			},
			ViewsByName: map[string]*plan.View{},
			ByPath:      map[string]*plan.Field{},
		},
	}
	loader := New()
	artifacts, err := loader.LoadViews(context.Background(), planned)
	require.NoError(t, err)
	require.NotNil(t, artifacts)
	require.Len(t, artifacts.Views, 1)
	actual := artifacts.Views[0]
	assert.Equal(t, "platform/items", actual.Module)
	require.NotNil(t, actual.AllowNulls)
	assert.True(t, *actual.AllowNulls)
	require.NotNil(t, actual.Selector)
	assert.Equal(t, "it", actual.Selector.Namespace)
	assert.True(t, actual.Selector.NoLimit)
	require.NotNil(t, actual.Schema)
	assert.Equal(t, "*ItemView", actual.Schema.DataType)
}

func TestLoader_LoadComponent(t *testing.T) {
	scanner := scan.New()
	scanned, err := scanner.Scan(context.Background(), &shape.Source{Name: "/v1/api/report", Struct: &reportSource{}})
	require.NoError(t, err)

	planner := plan.New()
	planned, err := planner.Plan(context.Background(), scanned)
	require.NoError(t, err)
	actualPlan, ok := plan.ResultFrom(planned)
	require.True(t, ok)
	actualPlan.ColumnsDiscovery = true
	actualPlan.TypeContext = &typectx.Context{
		DefaultPackage: "mdp/performance",
		Imports: []typectx.Import{
			{Alias: "perf", Package: "github.com/acme/mdp/performance"},
		},
	}
	actualPlan.Directives = &dqlshape.Directives{
		Meta:             "docs/report.md",
		DefaultConnector: "analytics",
		Cache: &dqlshape.CacheDirective{
			Enabled: true,
			TTL:     "5m",
		},
		MCP: &dqlshape.MCPDirective{
			Name:            "report.list",
			Description:     "List report rows",
			DescriptionPath: "docs/mcp/report.md",
		},
	}

	loader := New()
	artifact, err := loader.LoadComponent(context.Background(), planned)
	require.NoError(t, err)
	require.NotNil(t, artifact)
	require.NotNil(t, artifact.Resource)
	require.NotNil(t, artifact.Component)

	component, ok := ComponentFrom(artifact)
	require.True(t, ok)
	assert.Equal(t, "/v1/api/report", component.Name)
	assert.Equal(t, "/v1/api/report", component.URI)
	assert.Equal(t, "GET", component.Method)
	assert.Equal(t, "rows", component.RootView)
	assert.Equal(t, []string{"rows"}, component.Views)
	assert.Len(t, component.Input, 1)
	assert.Len(t, component.Output, 1)
	assert.Len(t, component.Async, 1)
	assert.Len(t, component.Meta, 1)
	require.NotNil(t, component.TypeContext)
	assert.Equal(t, "mdp/performance", component.TypeContext.DefaultPackage)
	require.Len(t, component.TypeContext.Imports, 1)
	assert.Equal(t, "perf", component.TypeContext.Imports[0].Alias)
	require.NotNil(t, component.Directives)
	assert.Equal(t, "docs/report.md", component.Directives.Meta)
	assert.Equal(t, "analytics", component.Directives.DefaultConnector)
	require.NotNil(t, component.Directives.Cache)
	assert.True(t, component.Directives.Cache.Enabled)
	assert.Equal(t, "5m", component.Directives.Cache.TTL)
	require.NotNil(t, component.Directives.MCP)
	assert.Equal(t, "report.list", component.Directives.MCP.Name)
	assert.True(t, component.ColumnsDiscovery)
}

func TestLoader_LoadComponent_RelationFieldsPreserved(t *testing.T) {
	planned := &shape.PlanResult{
		Source: &shape.Source{Name: "/v1/api/report"},
		Plan: &plan.Result{
			Views: []*plan.View{
				{
					Path:        "Rows",
					Name:        "rows",
					Table:       "REPORT",
					Cardinality: "many",
					FieldType:   reflect.TypeOf([]reportRow{}),
					ElementType: reflect.TypeOf(reportRow{}),
					Relations: []*plan.Relation{
						{
							Name:   "detail",
							Holder: "Detail",
							Ref:    "detail",
							Table:  "REPORT_DETAIL",
							On: []*plan.RelationLink{
								{
									ParentField:     "ReportID",
									ParentNamespace: "rows",
									ParentColumn:    "REPORT_ID",
									RefField:        "ID",
									RefNamespace:    "detail",
									RefColumn:       "ID",
								},
							},
						},
					},
				},
			},
			ViewsByName: map[string]*plan.View{},
			ByPath:      map[string]*plan.Field{},
		},
	}

	loader := New()
	artifact, err := loader.LoadComponent(context.Background(), planned)
	require.NoError(t, err)
	component, ok := ComponentFrom(artifact)
	require.True(t, ok)
	require.Len(t, component.ViewRelations, 1)
	require.Len(t, component.ViewRelations[0].On, 1)
	require.Len(t, component.ViewRelations[0].Of.On, 1)

	parent := component.ViewRelations[0].On[0]
	ref := component.ViewRelations[0].Of.On[0]
	assert.Equal(t, "ReportID", parent.Field)
	assert.Equal(t, "rows", parent.Namespace)
	assert.Equal(t, "REPORT_ID", parent.Column)
	assert.Equal(t, "ID", ref.Field)
	assert.Equal(t, "detail", ref.Namespace)
	assert.Equal(t, "ID", ref.Column)
}
