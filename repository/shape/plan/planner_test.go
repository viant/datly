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

func TestPlanner_Plan(t *testing.T) {
	scanner := scan.New()
	scanned, err := scanner.Scan(context.Background(), &shape.Source{Struct: &reportSource{}})
	require.NoError(t, err)

	planner := New()
	planned, err := planner.Plan(context.Background(), scanned)
	require.NoError(t, err)
	require.NotNil(t, planned)

	result, ok := planned.Plan.(*Result)
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
	assert.Equal(t, outputkeys.Types["status"], stateByPath["Status"].EffectiveType)
	require.NotNil(t, stateByPath["Job"])
	assert.Equal(t, asynckeys.Types["job"], stateByPath["Job"].EffectiveType)
	require.NotNil(t, stateByPath["VName"])
	assert.Equal(t, metakeys.Types["view.name"], stateByPath["VName"].EffectiveType)

	require.NotNil(t, stateByPath["ID"])
	assert.Equal(t, "query", stateByPath["ID"].Kind)
	assert.Equal(t, "id", stateByPath["ID"].In)
	assert.Equal(t, stateByPath["ID"].TagType, stateByPath["ID"].EffectiveType)
}

func TestPlanner_Plan_InvalidDescriptors(t *testing.T) {
	planner := New()
	_, err := planner.Plan(context.Background(), &shape.ScanResult{Source: &shape.Source{Name: "x"}, Descriptors: "invalid"})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "unsupported descriptors type")
}
