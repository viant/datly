package repository

import (
	"bytes"
	"context"
	"net/http"
	"net/http/httptest"
	"reflect"
	"testing"
	"time"

	_ "github.com/mattn/go-sqlite3"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/viant/datly/repository/contract"
	"github.com/viant/datly/repository/cubecompose"
	"github.com/viant/datly/repository/report"
	"github.com/viant/datly/view"
	"github.com/viant/datly/view/state"
)

type deadlineCapturePreparer struct {
	hasDeadline bool
}

func (d *deadlineCapturePreparer) Dispatch(context.Context, *contract.Path, ...contract.Option) (interface{}, error) {
	return nil, nil
}

func (d *deadlineCapturePreparer) PrepareQuery(ctx context.Context, _ *contract.Path, _ *http.Request) (*contract.PreparedQuery, error) {
	_, d.hasDeadline = ctx.Deadline()
	return nil, context.Canceled
}

func TestCubeComposeReadRows_UsesDynamicStructCollection(t *testing.T) {
	catalog, err := cubecompose.NewCatalog(
		cubecompose.Field{Name: "account_id", Type: reflect.TypeOf(int64(0)), Role: cubecompose.Dimension},
		cubecompose.Field{Name: "total", Type: reflect.TypeOf(float64(0)), Role: cubecompose.Measure},
	)
	require.NoError(t, err)
	plan, err := cubecompose.Compile(`SELECT t1.account_id,
 t1.total - t2.total AS value_delta
 FROM $CubeSQL1 AS t1
 JOIN $CubeSQL2 AS t2 ON t1.account_id = t2.account_id
 LIMIT 5`, catalog, 2, 10)
	require.NoError(t, err)

	resource := view.EmptyResource()
	connector := view.NewConnector("compose", "sqlite3", ":memory:")
	require.NoError(t, connector.Init(context.Background(), nil))
	rootView := &view.View{Name: "metrics", Connector: connector}
	rootView.SetResource(resource)
	handler := &cubeComposeHandler{Original: &Component{View: rootView}}

	data, err := handler.readRows(context.Background(), plan, `
SELECT 1 AS account_id, 12.5 AS value_delta
UNION ALL
SELECT 2 AS account_id, -3.0 AS value_delta`, nil)
	require.NoError(t, err)
	rows := reflect.ValueOf(data)
	require.Equal(t, reflect.Slice, rows.Kind())
	require.Len(t, data, 2)
	require.Equal(t, reflect.Ptr, rows.Type().Elem().Kind())
	require.Equal(t, reflect.Struct, rows.Type().Elem().Elem().Kind())
	assert.Equal(t, int64(1), rows.Index(0).Elem().FieldByName("AccountId").Int())
	assert.Equal(t, float64(12.5), rows.Index(0).Elem().FieldByName("ValueDelta").Interface())
}

func TestCubeComposeReadRows_AllowsNullFromLeftJoinedFrame(t *testing.T) {
	catalog, err := cubecompose.NewCatalog(
		cubecompose.Field{Name: "account_id", Type: reflect.TypeOf(int64(0)), Role: cubecompose.Dimension},
		cubecompose.Field{Name: "total", Type: reflect.TypeOf(float64(0)), Role: cubecompose.Measure},
	)
	require.NoError(t, err)
	plan, err := cubecompose.Compile(`SELECT t1.account_id,
 t2.total AS previous_total
 FROM $CubeSQL1 AS t1
 LEFT JOIN $CubeSQL2 AS t2 ON t1.account_id = t2.account_id
 LIMIT 5`, catalog, 2, 10)
	require.NoError(t, err)

	resource := view.EmptyResource()
	connector := view.NewConnector("compose-null", "sqlite3", ":memory:")
	require.NoError(t, connector.Init(context.Background(), nil))
	rootView := &view.View{Name: "metrics", Connector: connector}
	rootView.SetResource(resource)
	handler := &cubeComposeHandler{Original: &Component{View: rootView}}

	data, err := handler.readRows(context.Background(), plan, `SELECT 1 AS account_id, NULL AS previous_total`, nil)
	require.NoError(t, err)
	rows := reflect.ValueOf(data)
	require.Len(t, data, 1)
	assert.True(t, rows.Index(0).Elem().FieldByName("PreviousTotal").IsNil())
}

func TestCubeComposeExecAppliesTimeoutDuringFramePreparation(t *testing.T) {
	type frame struct {
		Filters struct{} `json:"filters"`
	}
	type body struct {
		Cubes []frame `json:"cubes"`
		SQL   string  `json:"sql"`
	}

	resource := view.EmptyResource()
	connector := view.NewConnector("compose-timeout", "sqlite3", ":memory:")
	require.NoError(t, connector.Init(context.Background(), nil))
	rootView := view.NewView("metrics", "METRICS")
	rootView.Connector = connector
	rootView.Selector = &view.Config{}
	rootView.Columns = []*view.Column{view.NewColumn("AccountID", "int64", reflect.TypeOf(int64(0)), false)}
	rootView.SetResource(resource)
	require.NoError(t, rootView.Init(context.Background(), resource))

	dispatcher := &deadlineCapturePreparer{}
	request := httptest.NewRequest(http.MethodPost, "http://localhost/v1/api/metrics/cube/compose", bytes.NewBufferString(
		`{"cubes":[{"filters":{}}],"sql":"SELECT t1.AccountID FROM $CubeSQL1 AS t1 LIMIT 1"}`,
	))
	handler := &cubeComposeHandler{
		Dispatcher: dispatcher,
		Path:       &contract.Path{Method: http.MethodGet, URI: "/v1/api/metrics"},
		Metadata:   &ReportMetadata{Dimensions: []*ReportField{{Name: "AccountID"}}},
		Original:   &Component{View: rootView},
		BodyType:   reflect.TypeOf(body{}),
		Config:     &CubeCompose{Enabled: true, MaxCubes: 1, MaxLimit: 10, TimeoutMs: 1000},
	}
	session := &reportTestSession{http: &reportTestHTTP{request: request}}

	_, err := handler.Exec(context.Background(), session)
	require.ErrorIs(t, err, context.Canceled)
	assert.True(t, dispatcher.hasDeadline)
}

func TestComposeFrameContext_UsesSharedSnapshot(t *testing.T) {
	type frame struct {
		Align string
	}
	snapshot := time.Date(2026, 9, 5, 18, 30, 0, 0, time.UTC)
	ctx, err := composeFrameContext(context.Background(), reflect.ValueOf(frame{Align: "elapsed"}), 2, snapshot)
	require.NoError(t, err)
	actual, ok := contract.CubeComposeFrameContextFrom(ctx)
	require.True(t, ok)
	assert.Equal(t, 2, actual.Frame)
	assert.Equal(t, contract.CubeComposeAlignmentElapsed, actual.Alignment)
	assert.Equal(t, snapshot, actual.Snapshot)
}

func TestComposeFrameContext_RejectsUnknownAlignment(t *testing.T) {
	type frame struct {
		Align string
	}
	_, err := composeFrameContext(context.Background(), reflect.ValueOf(frame{Align: "approximately"}), 2, time.Now())
	require.Error(t, err)
}

func TestCubeComposeFrameRequest_UsesOnlyTypedFrameFilters(t *testing.T) {
	type filters struct {
		AccountID *int
	}
	type frame struct {
		Filters filters
	}
	accountID := 101
	handler := &cubeComposeHandler{
		Path: &contract.Path{Method: http.MethodGet, URI: "/v1/api/performance"},
		Metadata: &ReportMetadata{
			FiltersKey: "Filters",
			Filters: []*report.Filter{{
				Name:      "accountID",
				FieldName: "AccountID",
				Parameter: &state.Parameter{In: state.NewQueryLocation("accountID")},
			}},
		},
		Original: &Component{View: &view.View{Selector: &view.Config{
			FieldsParameter:   &state.Parameter{In: state.NewQueryLocation("_fields")},
			CriteriaParameter: &state.Parameter{In: state.NewQueryLocation("criteria")},
		}}},
	}
	source := httptest.NewRequest(http.MethodPost, "http://localhost/v1/api/performance/cube/compose?accountID=999&tenant=trusted&_limit=1&_criteria=unsafe&criteria=unsafe", nil)
	request, err := handler.frameRequest(source, reflect.ValueOf(frame{Filters: filters{AccountID: &accountID}}), []string{"account_id", "total_spend"})
	require.NoError(t, err)

	query := request.URL.Query()
	assert.Equal(t, "101", query.Get("accountID"))
	assert.Equal(t, "trusted", query.Get("tenant"), "non-cube routing and authorization inputs remain available")
	assert.Equal(t, "account_id,total_spend", query.Get("_fields"))
	assert.Empty(t, query.Get("_limit"))
	assert.Empty(t, query.Get("_criteria"))
	assert.Empty(t, query.Get("criteria"))
}

func TestResolveComposeFrames_InheritsOnlyOmittedFiltersFromSelectedCube(t *testing.T) {
	type filters struct {
		AdvertiserID *int
		Period       *string
	}
	type frame struct {
		InheritFrom *int
		Filters     filters
	}
	advertiserID := 29
	yesterday := "yesterday"
	today := "today"
	first := 1
	cubes := []frame{
		{Filters: filters{AdvertiserID: &advertiserID, Period: &yesterday}},
		{InheritFrom: &first, Filters: filters{Period: &today}},
		{InheritFrom: &first},
	}

	actual, err := resolveComposeFrames(reflect.ValueOf(cubes))
	require.NoError(t, err)
	require.Len(t, actual, 3)
	second := actual[1].Interface().(frame)
	require.NotNil(t, second.Filters.AdvertiserID)
	assert.Equal(t, 29, *second.Filters.AdvertiserID)
	require.NotNil(t, second.Filters.Period)
	assert.Equal(t, "today", *second.Filters.Period)
	third := actual[2].Interface().(frame)
	require.NotNil(t, third.Filters.AdvertiserID)
	assert.Equal(t, 29, *third.Filters.AdvertiserID)
	require.NotNil(t, third.Filters.Period)
	assert.Equal(t, "yesterday", *third.Filters.Period)
}

func TestResolveComposeFrames_RejectsForwardInheritance(t *testing.T) {
	type frame struct {
		InheritFrom *int
		Filters     struct{}
	}
	third := 3
	_, err := resolveComposeFrames(reflect.ValueOf([]frame{{}, {InheritFrom: &third}, {}}))
	require.Error(t, err)
	assert.Contains(t, err.Error(), "preceding cube")
}
