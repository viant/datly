package shape_test

import (
	"context"
	"embed"
	"reflect"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	shape "github.com/viant/datly/repository/shape"
	shapeLoad "github.com/viant/datly/repository/shape/load"
	shapePlan "github.com/viant/datly/repository/shape/plan"
	shapeScan "github.com/viant/datly/repository/shape/scan"
)

//go:embed scan/testdata/*.sql
var parityFS embed.FS

type parityEmbedded struct{}

func (parityEmbedded) EmbedFS() *embed.FS { return &parityFS }

type parityRow struct {
	ID   int
	Name string
}

type paritySource struct {
	parityEmbedded
	Rows []parityRow `view:"rows,table=REPORT,connector=dev" sql:"uri=scan/testdata/report.sql"`
}

type parityJoinRow struct {
	ReportID int `source:"REPORT_ID"`
}

type parityJoinSource struct {
	parityEmbedded
	Rows []parityJoinRow `view:"rows,table=REPORT,connector=dev" sql:"uri=scan/testdata/report.sql" on:"ReportID:rows.REPORT_ID=ID:detail.ID"`
}

func TestEngineParity_StructPipeline(t *testing.T) {
	source := &paritySource{}
	scanner := shapeScan.New()
	planner := shapePlan.New()
	loader := shapeLoad.New()

	manualScan, err := scanner.Scan(context.Background(), &shape.Source{Name: "/v1/api/parity", Struct: source})
	require.NoError(t, err)
	manualPlan, err := planner.Plan(context.Background(), manualScan)
	require.NoError(t, err)
	manualViews, err := loader.LoadViews(context.Background(), manualPlan)
	require.NoError(t, err)

	engine := shape.New(
		shape.WithName("/v1/api/parity"),
		shape.WithScanner(scanner),
		shape.WithPlanner(planner),
		shape.WithLoader(loader),
	)
	engineViews, err := engine.LoadViews(context.Background(), source)
	require.NoError(t, err)

	require.Len(t, manualViews.Views, 1)
	require.Len(t, engineViews.Views, 1)

	mv := manualViews.Views[0]
	ev := engineViews.Views[0]
	assert.Equal(t, mv.Name, ev.Name)
	assert.Equal(t, mv.Table, ev.Table)
	assert.Equal(t, mv.Template.Source, ev.Template.Source)
	assert.Equal(t, mv.Template.SourceURL, ev.Template.SourceURL)
	assert.Equal(t, mv.Schema.Cardinality, ev.Schema.Cardinality)
	assert.Equal(t, reflect.TypeOf(mv.Schema.CompType()), reflect.TypeOf(ev.Schema.CompType()))
}

func TestEngineParity_Component_SourceTagFieldJoin(t *testing.T) {
	source := &parityJoinSource{}
	scanner := shapeScan.New()
	planner := shapePlan.New()
	loader := shapeLoad.New()

	engine := shape.New(
		shape.WithName("/v1/api/parity"),
		shape.WithScanner(scanner),
		shape.WithPlanner(planner),
		shape.WithLoader(loader),
	)
	artifact, err := engine.LoadComponent(context.Background(), source)
	require.NoError(t, err)
	require.NotNil(t, artifact)

	component, ok := artifact.Component.(*shapeLoad.Component)
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
