package repository

import (
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
 LIMIT 5`, catalog, 10)
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

func TestInheritComposeFrame_CopiesOnlyOmittedFilters(t *testing.T) {
	type filters struct {
		AdvertiserID *int
		Period       *string
	}
	type frame struct {
		Inherit bool
		Filters filters
	}
	advertiserID := 29
	yesterday := "yesterday"
	today := "today"
	cube1 := frame{Filters: filters{AdvertiserID: &advertiserID, Period: &yesterday}}
	cube2 := frame{Inherit: true, Filters: filters{Period: &today}}

	actual := inheritComposeFrame(reflect.ValueOf(cube1), reflect.ValueOf(cube2)).Interface().(frame)
	require.NotNil(t, actual.Filters.AdvertiserID)
	assert.Equal(t, 29, *actual.Filters.AdvertiserID)
	require.NotNil(t, actual.Filters.Period)
	assert.Equal(t, "today", *actual.Filters.Period)
}
