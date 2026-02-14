package load

import (
	"context"
	"embed"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/viant/datly/repository/shape"
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

func TestLoader_LoadViews_InvalidPlanType(t *testing.T) {
	loader := New()
	_, err := loader.LoadViews(context.Background(), &shape.PlanResult{Source: &shape.Source{Name: "x"}, Plan: "invalid"})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "unsupported plan type")
}

func TestLoader_LoadComponent(t *testing.T) {
	scanner := scan.New()
	scanned, err := scanner.Scan(context.Background(), &shape.Source{Name: "/v1/api/report", Struct: &reportSource{}})
	require.NoError(t, err)

	planner := plan.New()
	planned, err := planner.Plan(context.Background(), scanned)
	require.NoError(t, err)
	actualPlan, ok := planned.Plan.(*plan.Result)
	require.True(t, ok)
	actualPlan.TypeContext = &typectx.Context{
		DefaultPackage: "mdp/performance",
		Imports: []typectx.Import{
			{Alias: "perf", Package: "github.com/acme/mdp/performance"},
		},
	}

	loader := New()
	artifact, err := loader.LoadComponent(context.Background(), planned)
	require.NoError(t, err)
	require.NotNil(t, artifact)
	require.NotNil(t, artifact.Resource)
	require.NotNil(t, artifact.Component)

	component, ok := artifact.Component.(*Component)
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
}
