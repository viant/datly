package plan

import (
	"context"
	"embed"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	asynckeys "github.com/viant/datly/repository/locator/async/keys"
	metakeys "github.com/viant/datly/repository/locator/meta/keys"
	outputkeys "github.com/viant/datly/repository/locator/output/keys"
	"github.com/viant/datly/repository/shape"
	"github.com/viant/datly/repository/shape/scan"
)

//go:embed testdata/*.sql
var testFS embed.FS

type embeddedFS struct{}

func (embeddedFS) EmbedFS() *embed.FS {
	return &testFS
}

type reportRow struct {
	ID int
}

type reportSource struct {
	embeddedFS
	Rows   []reportRow `view:"rows,table=REPORT,connector=dev" sql:"uri=testdata/report.sql"`
	Status interface{} `parameter:"status,kind=output,in=status"`
	Job    interface{} `parameter:"job,kind=async,in=job"`
	VName  interface{} `parameter:"viewName,kind=meta,in=view.name"`
	ID     int         `parameter:"id,kind=query,in=id"`
}

type relationRow struct {
	ID int
}

type relationSource struct {
	Rows []relationRow `view:"rows,table=REPORT" on:"rows.report_id=report.id"`
}

type relationSourceWithFields struct {
	Rows []relationRow `view:"rows,table=REPORT" on:"ReportID:rows.report_id=ID:report.id"`
}

func TestPlanner_Plan(t *testing.T) {
	scanner := scan.New()
	scanned, err := scanner.Scan(context.Background(), &shape.Source{Struct: &reportSource{}})
	require.NoError(t, err)

	planner := New()
	planned, err := planner.Plan(context.Background(), scanned)
	require.NoError(t, err)
	require.NotNil(t, planned)

	result, ok := ResultFrom(planned)
	require.True(t, ok)
	require.NotNil(t, result)
	require.NotNil(t, result.EmbedFS)

	require.Len(t, result.Views, 1)
	rows := result.Views[0]
	assert.Equal(t, "rows", rows.Name)
	assert.Equal(t, "REPORT", rows.Table)
	assert.Equal(t, "dev", rows.Connector)
	assert.Equal(t, "many", rows.Cardinality)
	assert.Equal(t, "Rows", rows.Holder)
	assert.Contains(t, rows.SQL, "SELECT ID")

	stateByPath := map[string]*State{}
	for _, item := range result.States {
		stateByPath[item.Path] = item
	}

	require.NotNil(t, stateByPath["Status"])
	assert.Equal(t, outputkeys.Types["status"], stateByPath["Status"].Schema.Type())
	require.NotNil(t, stateByPath["Job"])
	assert.Equal(t, asynckeys.Types["job"], stateByPath["Job"].Schema.Type())
	require.NotNil(t, stateByPath["VName"])
	assert.Equal(t, metakeys.Types["view.name"], stateByPath["VName"].Schema.Type())

	require.NotNil(t, stateByPath["ID"])
	assert.Equal(t, "query", stateByPath["ID"].KindString())
	assert.Equal(t, "id", stateByPath["ID"].InName())
}

func TestPlanner_Plan_LinkOnProducesStructuredRelations(t *testing.T) {
	scanner := scan.New()
	scanned, err := scanner.Scan(context.Background(), &shape.Source{Struct: &relationSource{}})
	require.NoError(t, err)

	planner := New()
	planned, err := planner.Plan(context.Background(), scanned)
	require.NoError(t, err)
	require.NotNil(t, planned)

	result, ok := ResultFrom(planned)
	require.True(t, ok)
	require.Len(t, result.Views, 1)
	viewPlan := result.Views[0]
	require.Len(t, viewPlan.Relations, 1)
	relation := viewPlan.Relations[0]
	require.Len(t, relation.On, 1)
	assert.Equal(t, "rows", relation.On[0].ParentNamespace)
	assert.Equal(t, "report_id", relation.On[0].ParentColumn)
	assert.Equal(t, "report", relation.On[0].RefNamespace)
	assert.Equal(t, "id", relation.On[0].RefColumn)
}

func TestPlanner_Plan_LinkOnPreservesFieldSelectors(t *testing.T) {
	scanner := scan.New()
	scanned, err := scanner.Scan(context.Background(), &shape.Source{Struct: &relationSourceWithFields{}})
	require.NoError(t, err)

	planner := New()
	planned, err := planner.Plan(context.Background(), scanned)
	require.NoError(t, err)
	require.NotNil(t, planned)

	result, ok := ResultFrom(planned)
	require.True(t, ok)
	require.Len(t, result.Views, 1)
	viewPlan := result.Views[0]
	require.Len(t, viewPlan.Relations, 1)
	relation := viewPlan.Relations[0]
	require.Len(t, relation.On, 1)
	assert.Equal(t, "ReportID", relation.On[0].ParentField)
	assert.Equal(t, "rows", relation.On[0].ParentNamespace)
	assert.Equal(t, "report_id", relation.On[0].ParentColumn)
	assert.Equal(t, "ID", relation.On[0].RefField)
	assert.Equal(t, "report", relation.On[0].RefNamespace)
	assert.Equal(t, "id", relation.On[0].RefColumn)
}

// stubScanSpec is a non-scan-Result implementation of shape.ScanSpec used to
// verify that Plan() returns an error when given an unexpected descriptor type.
type stubScanSpec struct{}

func (s *stubScanSpec) ShapeSpecKind() string { return "stub" }

func TestPlanner_Plan_InvalidDescriptors(t *testing.T) {
	planner := New()
	_, err := planner.Plan(context.Background(), &shape.ScanResult{Source: &shape.Source{Name: "x"}, Descriptors: &stubScanSpec{}})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "unsupported descriptors kind")
}
