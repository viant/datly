package gateway

import (
	"reflect"
	"testing"

	"github.com/stretchr/testify/require"
	dqlshape "github.com/viant/datly/repository/shape/dql/shape"
	shapeLoad "github.com/viant/datly/repository/shape/load"
	shapePlan "github.com/viant/datly/repository/shape/plan"
	datlyservice "github.com/viant/datly/service"
	"github.com/viant/datly/view"
	"github.com/viant/datly/view/state"
)

func TestBootstrapReport(t *testing.T) {
	actual := bootstrapReport(&shapeLoad.Component{
		Report: &dqlshape.ReportDirective{
			Enabled:    true,
			Input:      "NamedReportInput",
			Dimensions: "Dims",
			Measures:   "Metrics",
			Filters:    "Predicates",
			OrderBy:    "Sort",
			Limit:      "Take",
			Offset:     "Skip",
		},
	})
	require.NotNil(t, actual)
	require.True(t, actual.Enabled)
	require.Equal(t, "NamedReportInput", actual.Input)
	require.Equal(t, "Dims", actual.Dimensions)
	require.Equal(t, "Metrics", actual.Measures)
	require.Equal(t, "Predicates", actual.Filters)
	require.Equal(t, "Sort", actual.OrderBy)
	require.Equal(t, "Take", actual.Limit)
	require.Equal(t, "Skip", actual.Offset)
}

func TestMergeBootstrapView_PreservesColumnGroupingMetadata(t *testing.T) {
	target := &view.View{
		Columns: []*view.Column{
			{Name: "ACCOUNT_ID"},
			{Name: "TOTAL_ID"},
		},
	}
	source := &view.View{
		Columns: []*view.Column{
			{Name: "ACCOUNT_ID", Groupable: true},
			{Name: "TOTAL_ID", Aggregate: true},
		},
	}

	mergeBootstrapView(target, source)

	require.True(t, target.Columns[0].Groupable)
	require.True(t, target.Columns[1].Aggregate)
}

func TestBootstrapRequiresRootView(t *testing.T) {
	require.False(t, bootstrapRequiresRootView(&shapeLoad.Component{}))
	require.True(t, bootstrapRequiresRootView(&shapeLoad.Component{RootView: "vendor"}))
	require.True(t, bootstrapRequiresRootView(&shapeLoad.Component{Views: []string{"vendor"}}))
	require.True(t, bootstrapRequiresRootView(&shapeLoad.Component{Report: &dqlshape.ReportDirective{Enabled: true}}))
}

func TestDefaultServiceForMethod_HandlerOnlyGETUsesExecutor(t *testing.T) {
	require.Equal(t, datlyservice.TypeExecutor, defaultServiceForMethod("GET", nil))
	require.Equal(t, datlyservice.TypeReader, defaultServiceForMethod("GET", &view.View{}))
}

func TestBootstrapHandlerView_UsesOutputViewSchema(t *testing.T) {
	payloadType := reflect.TypeOf(struct {
		UserID int
	}{})
	component := &shapeLoad.Component{
		Name: "LinkedAuth",
		Output: []*shapePlan.State{
			{Parameter: state.Parameter{Name: "Data", In: state.NewOutputLocation("view"), Schema: state.NewSchema(payloadType)}},
		},
	}

	resource := view.EmptyResource()
	resource.AddConnectors(view.NewConnector("dev", "sqlite3", "sqlite3://localhost/tmp/test.db"))

	handlerView := bootstrapHandlerView(resource, component, "linkedauth")
	require.NotNil(t, handlerView)
	require.Equal(t, "LinkedAuth", handlerView.Name)
	require.Equal(t, view.ModeHandler, handlerView.Mode)
	require.NotNil(t, handlerView.Connector)
	require.Equal(t, "dev", handlerView.Connector.Ref)
	require.NotNil(t, handlerView.Schema)
	require.Equal(t, payloadType, handlerView.Schema.Type())
}
