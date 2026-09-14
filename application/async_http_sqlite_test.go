package application_test

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"reflect"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/viant/datly/application"
	"github.com/viant/datly/bootstrap"
	gateway "github.com/viant/datly/gateway/http"
	authfixture "github.com/viant/datly/internal/testharness/auth"
	rauth "github.com/viant/datly/runtime/auth"
	rhandler "github.com/viant/datly/runtime/handler"
	"github.com/viant/datly/runtime/jobs"
	"github.com/viant/datly/runtime/registry"
	"github.com/viant/datly/spec"
	dsql "github.com/viant/datly/sql"
	sqldml "github.com/viant/datly/sql/dml"
	"github.com/viant/datly/typecatalog"
	"github.com/viant/scy/auth/jwt"
	xasync "github.com/viant/xdatly/async"
	xcodec "github.com/viant/xdatly/codec"
	xhandler "github.com/viant/xdatly/handler"
	xresponse "github.com/viant/xdatly/response"
)

type httpAsyncBody struct {
	Quantity int                           `json:"quantity"`
	Note     string                        `json:"note"`
	Has      struct{ Quantity, Note bool } `setMarker:"true" json:"-"`
}
type httpAsyncInput struct {
	JWT       *jwt.Claims
	Token     string
	InitCount int
	ID        int
	Key       string
	Sync      bool
	Body      *httpAsyncBody
	Current   *httpAsyncRows
}
type httpAsyncStatusInput struct {
	JobID string
	JWT   *jwt.Claims
	Token string
}
type httpAsyncRow struct {
	ID, Quantity int
	Note         string
}
type httpAsyncRows struct{ Data []*httpAsyncRow }
type httpAsyncOutput struct {
	Job      *xasync.Job `parameter:"kind=async,in=job" json:"job,omitempty"`
	Code     string      `parameter:"kind=async,in=jobinfo.code" json:"code"`
	Status   string      `parameter:"kind=output,in=status" json:"status"`
	Quantity int         `json:"quantity,omitempty"`
	Note     string      `json:"note,omitempty"`
}

func (i *httpAsyncInput) Init(context.Context) error { i.InitCount++; return nil }

type httpAsyncFixture struct {
	jwt         *authfixture.JWT
	token       string
	derivedJWT  bool
	handlerGate func(context.Context) error
	asyncAppFixture
	calls       atomic.Int32
	initialized atomic.Int32
	inputSeen   chan *httpAsyncInput
}

func (f *httpAsyncFixture) init(t *testing.T, watch bool) {
	f.asyncAppFixture.init(t, watch)
	require.NoError(t, f.db.ExecStatements(context.Background(), "CREATE TABLE inventory(ID INTEGER PRIMARY KEY,Quantity INTEGER,Note TEXT)", "INSERT INTO inventory VALUES(7,10,'before')"))
	f.inputSeen = make(chan *httpAsyncInput, 10)
}
func (f *httpAsyncFixture) compile(ctx context.Context, _ *typecatalog.Catalog) (*application.Build, error) {
	required := true
	current := &spec.Component{Key: spec.Key{Kind: spec.KindComponent, Name: "Current"}, Name: "Current", Routes: []*spec.Route{{Method: "GET", Path: "/current"}}, Parameters: []*spec.Parameter{{Name: "ID", Source: spec.BindSource{Kind: "query", Name: "id"}}, {Name: "Data", Source: spec.BindSource{Kind: "output", Name: "view"}}}, RootView: &spec.View{Name: "Current", Source: &spec.ViewSource{SQL: "SELECT ID,Quantity,Note FROM inventory WHERE ID=:ID"}}}
	child, err := bootstrap.BuildArtifact(bootstrap.ArtifactInput{Component: current, InputType: reflect.TypeOf(struct{ ID int }{}), OutputType: reflect.TypeOf(httpAsyncRows{}), DirectViewField: "Data"})
	if err != nil {
		return nil, err
	}
	reader, err := child.ReaderCompilation().NewExecution(bootstrap.ReaderRuntimeConfig{SQL: &dsql.SQLComponent{DB: f.db.DB}})
	if err != nil {
		return nil, err
	}
	parent := &spec.Component{Key: spec.Key{Kind: spec.KindComponent, Name: "Inventory"}, Name: "Inventory", Routes: []*spec.Route{{Method: "PATCH", Path: "/inventory", APIKeyHeader: "X-Key", APIKeyValue: "allowed"}}, Parameters: []*spec.Parameter{
		{Name: "ID", Source: spec.BindSource{Kind: "query", Name: "id"}, Required: &required},
		{Name: "Key", Source: spec.BindSource{Kind: "query", Name: "key"}, Required: &required},
		{Name: "Sync", Source: spec.BindSource{Kind: "query", Name: "wait"}},
		{Name: "Body", Source: spec.BindSource{Kind: "body"}, Required: &required},
		{Name: "Current", Source: spec.BindSource{Kind: "component", Name: "GET:/current"}},
	}}
	var factory xcodec.Factory
	if f.jwt != nil {
		factory = f.jwt.Factory
		parent.Parameters = append(parent.Parameters, f.jwtParameters()...)
	}
	artifact, err := bootstrap.BuildArtifact(bootstrap.ArtifactInput{CodecFactory: factory, Component: parent, InputType: reflect.TypeOf(httpAsyncInput{}), OutputType: reflect.TypeOf(httpAsyncOutput{})})
	if err != nil {
		return nil, err
	}
	handlerGate := f.handlerGate
	handler := rhandler.HandlerFunc(func(ctx context.Context, inv rhandler.Invocation) (any, error) {
		f.calls.Add(1)
		input := inv.Input.(*httpAsyncInput)
		if input.InitCount != 1 {
			return nil, fmt.Errorf("input initialization count: %d", input.InitCount)
		}
		if f.jwt != nil && (input.JWT == nil || input.JWT.Subject != "approved") {
			return nil, fmt.Errorf("unverified JWT reached handler")
		}
		if handlerGate != nil {
			if err := handlerGate(ctx); err != nil {
				return nil, err
			}
		}
		f.inputSeen <- input
		if input.Current == nil || len(input.Current.Data) != 1 {
			return nil, fmt.Errorf("current row missing")
		}
		if _, found, err := inv.Binder.Lookup(ctx, xhandler.ValueKey("jwt")); err != nil || found {
			return nil, fmt.Errorf("undeclared JWT became principal")
		}
		row := input.Current.Data[0]
		quantity, note := row.Quantity, row.Note
		if input.Body.Has.Quantity {
			quantity = input.Body.Quantity
		}
		if input.Body.Has.Note {
			note = input.Body.Note
		}
		value, _, err := inv.Binder.Lookup(ctx, xhandler.DataKey)
		if err != nil {
			return nil, err
		}
		if err = value.(xhandler.Data).Execute("UPDATE inventory SET Quantity=?,Note=? WHERE ID=?", quantity, note, input.ID); err != nil {
			return nil, err
		}
		return &httpAsyncOutput{Quantity: quantity, Note: note}, nil
	})
	inspect := &spec.Component{Key: spec.Key{Kind: spec.KindComponent, Name: "JobStatus"}, Name: "JobStatus", Routes: []*spec.Route{{Method: "GET", Path: "/job-status/{jobid}", APIKeyHeader: "X-Key", APIKeyValue: "allowed"}}, Parameters: []*spec.Parameter{{Name: "JobID", Source: spec.BindSource{Kind: "path", Name: "jobid"}, Required: &required}}}
	if f.jwt != nil {
		inspect.Parameters = append(inspect.Parameters, f.jwtParameters()...)
	}
	status, err := bootstrap.BuildArtifact(bootstrap.ArtifactInput{CodecFactory: factory, Component: inspect, InputType: reflect.TypeOf(httpAsyncStatusInput{}), OutputType: reflect.TypeOf(httpAsyncOutput{})})
	if err != nil {
		return nil, err
	}
	return &application.Build{Components: []*registry.RegisteredComponent{
		{Component: child.Component, Input: child.Input, Output: child.Output, OutputType: reflect.TypeOf(httpAsyncRows{}), Reader: reader},
		{Component: artifact.Component, Input: artifact.Input, Output: artifact.Output, OutputType: reflect.TypeOf(httpAsyncOutput{}), Handler: handler, DataSource: sqldml.Source{DB: f.db.DB}},
		{Component: status.Component, Input: status.Input, Output: status.Output, OutputType: reflect.TypeOf(httpAsyncOutput{}), Handler: rhandler.HandlerFunc(func(context.Context, rhandler.Invocation) (any, error) {
			return nil, fmt.Errorf("status handler must not execute")
		})},
	}, HTTP: gateway.Config{Async: []gateway.AsyncRoute{
		{Route: spec.RouteRef{Method: "PATCH", Path: "/inventory"}, MatchKey: "Key", SyncFlag: "Sync"},
		{Route: spec.RouteRef{Method: "GET", Path: "/job-status/{jobid}"}, Inspect: &gateway.AsyncInspect{JobID: "JobID", Target: spec.RouteRef{Method: "PATCH", Path: "/inventory"}}},
	}}}, nil
}
func (f *httpAsyncFixture) start(t *testing.T) {
	var err error
	f.manager, err = application.New(nil, application.WithAsync(f.config))
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, f.manager.Shutdown(context.Background())) })
	require.NoError(t, f.manager.Reload(context.Background(), application.Request{Revision: 1, Compile: f.compile}))
}
func (f *httpAsyncFixture) request(method, path, body, key string) *httptest.ResponseRecorder {
	req := httptest.NewRequest(method, path, strings.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("X-Key", key)
	token := f.token
	if token == "" {
		token = "undeclared-token-is-not-a-principal"
	}
	req.Header.Set("Authorization", "Bearer "+token)
	result := httptest.NewRecorder()
	f.manager.ServeHTTP(result, req)
	return result
}
func TestHTTPAsyncOwnedScheduleReplayStatusSQLite(t *testing.T) {
	f := &httpAsyncFixture{}
	f.init(t, true)
	entered, release := make(chan struct{}, 1), make(chan struct{})
	defer func() {
		select {
		case <-release:
		default:
			close(release)
		}
	}()
	f.config.Authorize = func(ctx context.Context, access jobs.Access) error {
		if input, ok := access.Input.(*httpAsyncInput); ok && input.Current != nil {
			return fmt.Errorf("current state read during capture")
		}
		if access.Action == jobs.Replay {
			select {
			case entered <- struct{}{}:
			default:
			}
			select {
			case <-release:
			case <-ctx.Done():
				return ctx.Err()
			}
		}
		return nil
	}
	f.start(t)
	result := f.request("PATCH", "/inventory?id=7&key=k1", `{"quantity":0}`, "allowed")
	require.Equal(t, 200, result.Code, result.Body.String())
	var pending httpAsyncOutput
	require.NoError(t, json.Unmarshal(result.Body.Bytes(), &pending))
	require.Equal(t, "WAITING", pending.Code)
	require.Equal(t, "ok", pending.Status)
	require.NotEmpty(t, pending.Job.ID)
	require.Zero(t, f.calls.Load())
	require.NotEmpty(t, result.Header().Get("Datly-Service-Time"))
	select {
	case <-entered:
	case <-time.After(5 * time.Second):
		t.Fatal("watcher did not receive job")
	}
	row, err := f.store.Get(context.Background(), pending.Job.ID)
	require.NoError(t, err)
	var state map[string]json.RawMessage
	require.NoError(t, json.Unmarshal([]byte(row.State), &state))
	require.JSONEq(t, `{"quantity":0}`, string(state["Body"]))
	require.NotContains(t, state, "Current")
	require.NotContains(t, state, "JWT")
	require.NoError(t, f.db.ExecStatements(context.Background(), "UPDATE inventory SET Quantity=42,Note='changed while pending' WHERE ID=7"))
	close(release)
	require.Eventually(t, func() bool {
		job, err := f.store.Get(context.Background(), pending.Job.ID)
		return err == nil && job.Status == xasync.StatusDone
	}, 5*time.Second, 5*time.Millisecond)
	status := f.request("GET", "/job-status/"+pending.Job.ID, "", "allowed")
	require.Equal(t, 200, status.Code, status.Body.String())
	var inspected httpAsyncOutput
	require.NoError(t, json.Unmarshal(status.Body.Bytes(), &inspected))
	require.Equal(t, "COMPLETE", inspected.Code)
	var quantity int
	var note string
	require.NoError(t, f.db.DB.QueryRow("SELECT Quantity,Note FROM inventory WHERE ID=7").Scan(&quantity, &note))
	require.Equal(t, 0, quantity)
	require.Equal(t, "changed while pending", note)
	repeat := f.request("PATCH", "/inventory?id=7&key=k1", `{"quantity":999}`, "allowed")
	require.Equal(t, 200, repeat.Code, repeat.Body.String())
	require.NoError(t, json.Unmarshal(repeat.Body.Bytes(), &inspected))
	require.Equal(t, pending.Job.ID, inspected.Job.ID)
	require.EqualValues(t, 1, f.calls.Load())
}
func TestHTTPAsyncSyncAndAccessSQLite(t *testing.T) {
	f := &httpAsyncFixture{}
	f.init(t, true)
	var denied atomic.Bool
	f.config.Authorize = func(_ context.Context, access jobs.Access) error {
		if denied.Load() {
			return &xresponse.Error{Code: 403, Payload: xresponse.Status{Status: "error", Message: "denied"}}
		}
		return nil
	}
	f.start(t)
	denied.Store(true)
	deniedResponse := f.request("PATCH", "/inventory?id=7&key=denied&wait=true", `{"note":"denied"}`, "allowed")
	require.Equal(t, 403, deniedResponse.Code, deniedResponse.Body.String())
	denied.Store(false)
	forbidden := f.request("PATCH", "/inventory?id=7&key=keyless", `{"note":"forbidden"}`, "wrong")
	require.Equal(t, 403, forbidden.Code)
	require.Zero(t, f.calls.Load())
	result := f.request("PATCH", "/inventory?id=7&key=sync&wait=true", `{"note":"sync result"}`, "allowed")
	require.Equal(t, 200, result.Code, result.Body.String())
	var output httpAsyncOutput
	require.NoError(t, json.Unmarshal(result.Body.Bytes(), &output))
	require.Equal(t, "COMPLETE", output.Code)
	require.Equal(t, "sync result", output.Note)
	require.EqualValues(t, 1, f.calls.Load())
	exists, err := f.fs.Exists(context.Background(), output.Job.EventURL)
	require.NoError(t, err)
	require.False(t, exists, "synchronous work was published to watcher")
	denied.Store(true)
	status := f.request("GET", "/job-status/"+output.Job.ID, "", "allowed")
	require.Equal(t, 403, status.Code, status.Body.String())
}

var _ http.Handler = (*application.Manager)(nil)

func (f *httpAsyncFixture) jwtParameters() []*spec.Parameter {
	required := true
	claim := &spec.Parameter{Name: "JWT", TypeExpr: "string", Source: spec.BindSource{Kind: "header", Name: "Authorization"}, Codec: &spec.Codec{Body: rauth.JwtClaim}, Required: &required, ErrorStatusCode: 401, ErrorMessage: "invalid authentication"}
	result := []*spec.Parameter{claim}
	if f.derivedJWT {
		claim.Source = spec.BindSource{Kind: "param", Name: "Token"}
		claim.TypeExpr = ""
		result = append(result, &spec.Parameter{Name: "Token", Source: spec.BindSource{Kind: "header", Name: "Authorization"}, Required: &required, ErrorStatusCode: 401, ErrorMessage: "invalid authentication"})
	}
	return result
}
