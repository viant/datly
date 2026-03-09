package repository

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"reflect"
	"testing"

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

func testReportHandler() *reportHandler {
	return &reportHandler{
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
	query, err := handler.buildQuery(testReportInput())
	require.NoError(t, err)
	assert.Equal(t, "AccountID,TotalSpend", query.Get("_fields"))
	assert.Equal(t, "AccountID", query.Get("_orderby"))
	assert.Equal(t, "25", query.Get("_limit"))
	assert.Equal(t, "101", query.Get("accountID"))
}

func TestReportHandler_Exec_PreservesAuthorizationHeader(t *testing.T) {
	handler := testReportHandler()
	handler.Metadata.Filters[0].Parameter = &state.Parameter{In: state.NewQueryLocation("accountID")}

	req := httptest.NewRequest(http.MethodPost, "http://localhost/v1/api/vendors/report", nil)
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
