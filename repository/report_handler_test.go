package repository

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"reflect"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/viant/datly/repository/contract"
	"github.com/viant/datly/view"
	"github.com/viant/datly/view/state"
	xhandler "github.com/viant/xdatly/handler"
	xdauth "github.com/viant/xdatly/handler/auth"
	"github.com/viant/xdatly/handler/differ"
	xdhttp "github.com/viant/xdatly/handler/http"
	xdlogger "github.com/viant/xdatly/handler/logger"
	"github.com/viant/xdatly/handler/mbus"
	"github.com/viant/xdatly/handler/sqlx"
	xdstate "github.com/viant/xdatly/handler/state"
	"github.com/viant/xdatly/handler/validator"
)

type captureDispatcher struct {
	path    *contract.Path
	options *contract.Options
}

func (d *captureDispatcher) Dispatch(ctx context.Context, path *contract.Path, options ...contract.Option) (interface{}, error) {
	d.path = path
	d.options = contract.NewOptions(options...)
	return map[string]string{"status": "ok"}, nil
}

type reportTestHTTP struct {
	request         *http.Request
	redirectRoute   *xdhttp.Route
	redirectRequest *http.Request
}

func (h *reportTestHTTP) RequestOf(ctx context.Context, v any) (*http.Request, error) {
	return h.request, nil
}
func (h *reportTestHTTP) NewRequest(ctx context.Context, opts ...xdstate.Option) (*http.Request, error) {
	return h.request, nil
}
func (h *reportTestHTTP) Redirect(ctx context.Context, route *xdhttp.Route, request *http.Request) error {
	h.redirectRoute = route
	h.redirectRequest = request
	return nil
}
func (h *reportTestHTTP) FailWithCode(statusCode int, err error) error { return err }

type reportTestLogger struct{}

func (l *reportTestLogger) IsDebugEnabled() bool                                       { return false }
func (l *reportTestLogger) IsInfoEnabled() bool                                        { return false }
func (l *reportTestLogger) IsWarnEnabled() bool                                        { return false }
func (l *reportTestLogger) IsErrorEnabled() bool                                       { return false }
func (l *reportTestLogger) Info(msg string, args ...any)                               {}
func (l *reportTestLogger) Debug(msg string, args ...any)                              {}
func (l *reportTestLogger) Warn(msg string, args ...any)                               {}
func (l *reportTestLogger) Error(msg string, args ...any)                              {}
func (l *reportTestLogger) Infoc(ctx context.Context, msg string, args ...any)         {}
func (l *reportTestLogger) Debugc(ctx context.Context, msg string, args ...any)        {}
func (l *reportTestLogger) DebugJSONc(ctx context.Context, msg string, obj any)        {}
func (l *reportTestLogger) Warnc(ctx context.Context, msg string, args ...any)         {}
func (l *reportTestLogger) Errorc(ctx context.Context, msg string, args ...any)        {}
func (l *reportTestLogger) Infos(ctx context.Context, msg string, attrs ...slog.Attr)  {}
func (l *reportTestLogger) Debugs(ctx context.Context, msg string, attrs ...slog.Attr) {}
func (l *reportTestLogger) Warns(ctx context.Context, msg string, attrs ...slog.Attr)  {}
func (l *reportTestLogger) Errors(ctx context.Context, msg string, attrs ...slog.Attr) {}

type reportTestSession struct {
	http   *reportTestHTTP
	logger xdlogger.Logger
}

type reportHandlerDimensions struct {
	AccountID bool
}

type reportHandlerMeasures struct {
	TotalSpend bool
}

type reportHandlerForecastingDimensions struct {
	AgegroupId bool
}

type reportHandlerForecastingMeasures struct {
	Avails bool
}

type reportHandlerFilters struct {
	AccountID *int
}

type reportHandlerBody struct {
	Dimensions reportHandlerDimensions
	Measures   reportHandlerMeasures
	Filters    reportHandlerFilters
	OrderBy    []string
	Limit      *int
	Offset     *int
}

type reportHandlerAdvancedFilters struct {
	Created  *time.Time `format:"dateFormat=YYYY-MM-DD"`
	Enabled  *bool
	Count    *int
	Code     reportHandlerStringerFilter
	Metadata *reportHandlerUnsupportedFilter
}

type reportHandlerAdvancedBody struct {
	Dimensions reportHandlerDimensions
	Measures   reportHandlerMeasures
	Filters    reportHandlerAdvancedFilters
}

type reportHandlerStringerFilter string

func (f reportHandlerStringerFilter) String() string {
	return "stringer:" + string(f)
}

type reportHandlerUnsupportedFilter struct {
	Value string
}

func (s *reportTestSession) Validator() *validator.Service                 { return nil }
func (s *reportTestSession) Differ() *differ.Service                       { return nil }
func (s *reportTestSession) MessageBus() *mbus.Service                     { return nil }
func (s *reportTestSession) Db(opts ...sqlx.Option) (*sqlx.Service, error) { return nil, nil }
func (s *reportTestSession) Stater() *xdstate.Service                      { return nil }
func (s *reportTestSession) FlushTemplate(ctx context.Context) error       { return nil }
func (s *reportTestSession) Session(ctx context.Context, route *xdhttp.Route, opts ...xdstate.Option) (xhandler.Session, error) {
	return s, nil
}
func (s *reportTestSession) Http() xdhttp.Http       { return s.http }
func (s *reportTestSession) Auth() xdauth.Auth       { return nil }
func (s *reportTestSession) Logger() xdlogger.Logger { return s.logger }

func testReportHandler() *cubeHandler {
	return &cubeHandler{
		Dispatcher: &captureDispatcher{},
		Path:       &contract.Path{Method: http.MethodGet, URI: "/v1/api/vendors"},
		Metadata: &ReportMetadata{
			BodyFieldName: "",
			DimensionsKey: "Dimensions",
			MeasuresKey:   "Measures",
			FiltersKey:    "Filters",
			OrderBy:       "OrderBy",
			Limit:         "Limit",
			Offset:        "Offset",
			Dimensions:    []*ReportField{{Name: "AccountID", FieldName: "AccountID", Section: "Dimensions"}},
			Measures:      []*ReportField{{Name: "TotalSpend", FieldName: "TotalSpend", Section: "Measures"}},
			Filters:       []*ReportFilter{{Name: "accountID", FieldName: "AccountID"}},
		},
		Original: &Component{
			View: &view.View{
				Selector: &view.Config{
					FieldsParameter:  &state.Parameter{In: state.NewQueryLocation("_fields")},
					OrderByParameter: &state.Parameter{In: state.NewQueryLocation("_orderby")},
					LimitParameter:   &state.Parameter{In: state.NewQueryLocation("_limit")},
					OffsetParameter:  &state.Parameter{In: state.NewQueryLocation("_offset")},
				},
			},
		},
	}
}

func testReportInput() reportHandlerBody {
	accountID := 101
	limit := 25
	return reportHandlerBody{
		Dimensions: reportHandlerDimensions{AccountID: true},
		Measures:   reportHandlerMeasures{TotalSpend: true},
		Filters:    reportHandlerFilters{AccountID: &accountID},
		OrderBy:    []string{"AccountID"},
		Limit:      &limit,
	}
}

func TestReportHandler_BuildQuery_FromPostBody(t *testing.T) {
	handler := testReportHandler()
	handler.Metadata.Filters[0].Parameter = &state.Parameter{In: state.NewQueryLocation("accountID")}
	req := httptest.NewRequest(http.MethodPost, "http://localhost/v1/api/vendors/cube", nil)
	query, err := handler.buildQuery(testReportInput(), req)
	require.NoError(t, err)
	assert.Equal(t, "AccountID,TotalSpend", query.Get("_fields"))
	assert.Equal(t, "AccountID", query.Get("_orderby"))
	assert.Equal(t, "25", query.Get("_limit"))
	assert.Equal(t, "101", query.Get("accountID"))
}

func TestReportHandler_BuildQuery_AutoIncludesRelationHolderForSelectedDimension(t *testing.T) {
	handler := &cubeHandler{
		Metadata: &ReportMetadata{
			DimensionsKey: "Dimensions",
			MeasuresKey:   "Measures",
			FiltersKey:    "Filters",
			OrderBy:       "OrderBy",
			Limit:         "Limit",
			Offset:        "Offset",
			Dimensions: []*ReportField{
				{Name: "AgegroupId", FieldName: "AgegroupId", Section: "Dimensions"},
			},
			Measures: []*ReportField{
				{Name: "Avails", FieldName: "Avails", Section: "Measures"},
			},
		},
		Original: &Component{
			View: &view.View{
				With: []*view.Relation{
					{
						Holder: "AgeGroup",
						On: view.Links{
							&view.Link{Field: "AgegroupId", Column: "agegroup_id"},
						},
					},
				},
				Selector: &view.Config{
					FieldsParameter: &state.Parameter{In: state.NewQueryLocation("_fields")},
				},
			},
		},
	}

	input := struct {
		Dimensions reportHandlerForecastingDimensions
		Measures   reportHandlerForecastingMeasures
	}{
		Dimensions: reportHandlerForecastingDimensions{AgegroupId: true},
		Measures:   reportHandlerForecastingMeasures{Avails: true},
	}

	req := httptest.NewRequest(http.MethodPost, "http://localhost/v1/api/steward/inventory/forecasting/cube", nil)
	query, err := handler.buildQuery(input, req)
	require.NoError(t, err)
	assert.Equal(t, "AgegroupId,Avails,AgeGroup", query.Get("_fields"))
}

func TestReportHandler_Exec_PreservesAuthorizationHeader(t *testing.T) {
	handler := testReportHandler()
	handler.Metadata.Filters[0].Parameter = &state.Parameter{In: state.NewQueryLocation("accountID")}

	req := httptest.NewRequest(http.MethodPost, "http://localhost/v1/api/vendors/cube", nil)
	req.Header.Set("Authorization", "Bearer test-token")
	httpSession := &reportTestHTTP{request: req}
	session := &reportTestSession{
		http:   httpSession,
		logger: &reportTestLogger{},
	}

	ctx := context.WithValue(context.Background(), xhandler.InputKey, testReportInput())
	_, err := handler.Exec(ctx, session)
	require.NoError(t, err)
	require.NotNil(t, httpSession.redirectRoute)
	require.NotNil(t, httpSession.redirectRequest)
	assert.Equal(t, "Bearer test-token", httpSession.redirectRequest.Header.Get("Authorization"))
	assert.Equal(t, "/v1/api/vendors", httpSession.redirectRequest.URL.Path)
	assert.Equal(t, http.MethodGet, httpSession.redirectRoute.Method)
	assert.Equal(t, "/v1/api/vendors", httpSession.redirectRoute.URL)
	query := httpSession.redirectRequest.URL.Query()
	assert.Equal(t, "AccountID,TotalSpend", query.Get("_fields"))
	assert.Equal(t, "AccountID", query.Get("_orderby"))
	assert.Equal(t, "25", query.Get("_limit"))
	assert.Equal(t, "101", query.Get("accountID"))
}

func TestReportHandler_ReportInput_AcceptsUnwrappedBody(t *testing.T) {
	handler := testReportHandler()
	handler.BodyType = reflect.TypeOf(&reportHandlerBody{})
	payload, err := json.Marshal(testReportInput())
	require.NoError(t, err)
	req := httptest.NewRequest(http.MethodPost, "http://localhost/v1/api/vendors/report", io.NopCloser(bytes.NewReader(payload)))
	input, err := handler.reportInput(context.Background(), req)
	require.NoError(t, err)
	body, ok := input.(*reportHandlerBody)
	require.True(t, ok)
	require.True(t, body.Dimensions.AccountID)
	require.True(t, body.Measures.TotalSpend)
}

func TestReportHandler_BuildQuery_FilterSerialization_DataDriven(t *testing.T) {
	created := time.Date(2026, time.April, 29, 14, 30, 0, 0, time.UTC)
	enabled := false
	count := 0
	cases := []struct {
		name          string
		filter        *ReportFilter
		filters       reportHandlerAdvancedFilters
		wantQuery     map[string]string
		wantMissing   []string
		wantErrSubstr string
	}{
		{
			name:   "formats time with field format tag",
			filter: &ReportFilter{Name: "created", FieldName: "Created", Parameter: &state.Parameter{In: state.NewQueryLocation("created")}},
			filters: reportHandlerAdvancedFilters{
				Created: &created,
			},
			wantQuery: map[string]string{"created": "2026-04-29"},
		},
		{
			name:   "preserves explicit false pointer",
			filter: &ReportFilter{Name: "enabled", FieldName: "Enabled", Parameter: &state.Parameter{In: state.NewQueryLocation("enabled")}},
			filters: reportHandlerAdvancedFilters{
				Enabled: &enabled,
			},
			wantQuery: map[string]string{"enabled": "false"},
		},
		{
			name:   "preserves explicit zero pointer",
			filter: &ReportFilter{Name: "count", FieldName: "Count", Parameter: &state.Parameter{In: state.NewQueryLocation("count")}},
			filters: reportHandlerAdvancedFilters{
				Count: &count,
			},
			wantQuery: map[string]string{"count": "0"},
		},
		{
			name:   "uses stringer for named value",
			filter: &ReportFilter{Name: "code", FieldName: "Code", Parameter: &state.Parameter{In: state.NewQueryLocation("code")}},
			filters: reportHandlerAdvancedFilters{
				Code: reportHandlerStringerFilter("A1"),
			},
			wantQuery: map[string]string{"code": "stringer:A1"},
		},
		{
			name:   "omits nil pointer filter",
			filter: &ReportFilter{Name: "created", FieldName: "Created", Parameter: &state.Parameter{In: state.NewQueryLocation("created")}},
			filters: reportHandlerAdvancedFilters{
				Created: nil,
			},
			wantMissing: []string{"created"},
		},
		{
			name:   "errors on unsupported present struct",
			filter: &ReportFilter{Name: "metadata", FieldName: "Metadata", Parameter: &state.Parameter{In: state.NewQueryLocation("metadata")}},
			filters: reportHandlerAdvancedFilters{
				Metadata: &reportHandlerUnsupportedFilter{Value: "x"},
			},
			wantErrSubstr: `report filter "Metadata"`,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			handler := testReportHandler()
			handler.Metadata.Filters = []*ReportFilter{tc.filter}

			input := reportHandlerAdvancedBody{
				Dimensions: reportHandlerDimensions{AccountID: true},
				Filters:    tc.filters,
			}

			req := httptest.NewRequest(http.MethodPost, "http://localhost/v1/api/vendors/cube", nil)
			query, err := handler.buildQuery(input, req)
			if tc.wantErrSubstr != "" {
				require.Error(t, err)
				assert.Contains(t, err.Error(), tc.wantErrSubstr)
				assert.Contains(t, err.Error(), fmt.Sprintf(`query param %q`, tc.filter.Parameter.In.Name))
				return
			}

			require.NoError(t, err)
			for key, want := range tc.wantQuery {
				assert.Equal(t, want, query.Get(key))
			}
			for _, key := range tc.wantMissing {
				assert.Empty(t, query.Get(key))
			}
		})
	}
}
