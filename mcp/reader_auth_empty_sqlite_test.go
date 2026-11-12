package mcp

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"github.com/viant/datly/exec"
	"github.com/viant/xdatly/response"
	"net/http/httptest"
	"reflect"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/viant/datly/bootstrap"
	gateway "github.com/viant/datly/gateway/http"
	"github.com/viant/datly/internal/testharness/mcpclient"
	"github.com/viant/datly/internal/testharness/sqlite"
	druntime "github.com/viant/datly/runtime"
	"github.com/viant/datly/runtime/registry"
	"github.com/viant/datly/spec"
	dsql "github.com/viant/datly/sql"
	sqlreader "github.com/viant/datly/sql/reader"
	"github.com/viant/datly/typecatalog"
	"github.com/viant/mcp-protocol/schema"
	"github.com/viant/x"
	xpredicate "github.com/viant/xdatly/predicate"
	xreader "github.com/viant/xdatly/reader"
)

var authEmptyCalls atomic.Int64

type authEmptyPredicate struct{}

func (*authEmptyPredicate) Compute(ctx context.Context, value any) (*xpredicate.Criteria, error) {
	authEmptyCalls.Add(1)
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	return &xpredicate.Criteria{Expression: "tenant = ?", Placeholders: []any{value}}, nil
}

func TestReaderAuthEmptyHTTPAndMCP(t *testing.T) {
	type claim struct{ UserID int }
	type input struct {
		Claim  claim
		Offset int    `parameter:"Offset,kind=query,in=offset" querySelector:"view=Records,property=offset" json:"offset"`
		Limit  int    `parameter:"Limit,kind=query,in=limit,value=10" querySelector:"view=Records,property=limit" json:"limit"`
		Key    []int  `parameter:"Key,kind=path,in=key" predicate:"in,r,id" json:"key"`
		Tenant int    `parameter:"Tenant,kind=query,in=tenant,value=7" predicate:"handler,example.com/auth.Scope,auth=true" json:"tenant"`
		Name   string `parameter:"Name,kind=query,in=name" predicate:"equal,r,name" json:"name"`
	}
	type row struct {
		ID int `sqlx:"id,primaryKey=true" json:"id"`
	}
	type output struct {
		Rows []row `json:"rows"`
	}
	db := sqlite.New(t)
	require.NoError(t, db.ExecStatements(context.Background(), "CREATE TABLE records(id INTEGER, tenant INTEGER, name TEXT, archived INTEGER)", "INSERT INTO records VALUES(1,7,'one',0),(2,8,'two',0),(3,8,'archived',1)", "CREATE TABLE accounts(id INTEGER,name TEXT)", "INSERT INTO accounts VALUES(1,'account')", "CREATE TABLE watches(record_id INTEGER,contact_id INTEGER)", "INSERT INTO watches VALUES(1,0)", "CREATE TABLE model_status(record_id INTEGER,status INTEGER)", "INSERT INTO model_status VALUES(1,2)"))
	types := typecatalog.NewCatalog()
	require.NoError(t, types.Register(typecatalog.TypeOriginPackage, &x.Type{Type: reflect.TypeFor[authEmptyPredicate](), PkgPath: "example.com/auth", Name: "Scope"}))
	component := &spec.Component{Key: spec.Key{Kind: spec.KindComponent, Name: "AuthEmpty"}, RootView: &spec.View{Name: "Records", Selector: &spec.Selector{AllowOffset: true, AllowLimit: true}, Source: &spec.ViewSource{SQL: `SELECT r.id, acc.name AS account_name,
 EXISTS(SELECT 1 FROM watches w WHERE w.record_id = r.id AND w.contact_id = ${Claim.UserID}) AS watching,
 (SELECT CASE WHEN COUNT(*) = 1 THEN MAX(m.status) ELSE 0 END FROM model_status m WHERE m.record_id = r.id) AS model_status
 FROM records r JOIN accounts acc ON acc.id = 1
 WHERE archived = 0 ${predicate.Builder().CombineAnd($predicate.FilterGroup(0,"AND")).Build("AND")}`}}, Routes: []*spec.Route{
		{Method: "GET", Path: "/records/{key}", MCP: []*spec.MCPExposure{{Kind: spec.MCPExposureTool, Name: "records.get"}}},
		{Method: "GET", Path: "/records", MCP: []*spec.MCPExposure{{Kind: spec.MCPExposureTool, Name: "records.list"}}},
	}}
	artifact, err := bootstrap.BuildArtifact(bootstrap.ArtifactInput{Component: component, InputType: reflect.TypeFor[input](), OutputType: reflect.TypeFor[output](), DirectViewField: "Rows", Types: types})
	require.NoError(t, err)
	execution, err := sqlreader.NewExecution(sqlreader.Config{Component: artifact.Component, InputType: reflect.TypeFor[input](), OutputType: reflect.TypeFor[output](), Plan: artifact.Reader, SQL: &dsql.SQLComponent{DB: db.DB}})
	require.NoError(t, err)
	var queries []string
	var failProbe error
	var cancelProbe context.CancelFunc
	guarded := execution.WithQueryGuard(func(_ context.Context, _ *sql.DB, q *xreader.ParameterizedQuery) error {
		queries = append(queries, q.Query)
		if len(queries) == 2 {
			if cancelProbe != nil {
				cancelProbe()
			}
			return failProbe
		}
		return nil
	})
	registered := &registry.RegisteredComponent{Component: artifact.Component, Input: artifact.Input, OutputType: reflect.TypeFor[output](), Reader: guarded}
	app, err := druntime.NewRuntime([]*registry.RegisteredComponent{registered})
	require.NoError(t, err)
	service, err := New(Config{Components: []*registry.RegisteredComponent{registered}, Invoker: app})
	require.NoError(t, err)
	native := mcpclient.New(t, service, schema.LatestProtocolVersion)
	for _, tc := range []struct {
		name, path      string
		ids             []int
		filter          string
		status, queries int
	}{
		{"allowed", "1", []int{1}, "", 200, 1},
		{"denied", "2", []int{2}, "", 403, 2},
		{"missing", "99", []int{99}, "", 404, 2},
		{"archived", "3", []int{3}, "", 404, 2},
		{"ordinary filter", "2", []int{2}, "absent", 404, 2},
		{"partial batch", "1,2", []int{1, 2}, "", 200, 1},
		{"denied batch", "2,99", []int{2, 99}, "", 403, 2},
		{"missing batch", "98,99", []int{98, 99}, "", 404, 2},
	} {
		t.Run(tc.name, func(t *testing.T) {
			queries = nil
			authEmptyCalls.Store(0)
			recorder := httptest.NewRecorder()
			gateway.NewHandler(app, nil, "").ServeHTTP(recorder, httptest.NewRequest("GET", "/records/"+tc.path+"?name="+tc.filter, nil))
			require.Equal(t, tc.status, recorder.Code, recorder.Body.String())
			require.Len(t, queries, tc.queries)
			require.EqualValues(t, 1, authEmptyCalls.Load())
			if tc.queries == 2 {
				require.Contains(t, queries[1], "SELECT 1 AS datly_exists")
				require.Contains(t, queries[1], "LIMIT 1")
				require.NotContains(t, queries[1], "tenant =")
				require.Contains(t, queries[1], "archived = 0")
			}
			queries = nil
			authEmptyCalls.Store(0)
			args := map[string]any{"key": tc.ids}
			if tc.filter != "" {
				args["name"] = tc.filter
			}
			result, err := native.CallTool(context.Background(), &schema.CallToolRequestParams{Name: "records.get", Arguments: args})
			require.NoError(t, err)
			require.NotNil(t, result)
			encoded, err := json.Marshal(result)
			require.NoError(t, err)
			if tc.status == 200 {
				require.True(t, result.IsError == nil || !*result.IsError, string(encoded))
			} else {
				require.NotNil(t, result.IsError, string(encoded))
				require.True(t, *result.IsError, string(encoded))
				require.Contains(t, string(encoded), map[int]string{403: "Forbidden", 404: "Not Found"}[tc.status])
			}
			require.Len(t, queries, tc.queries, string(encoded))
			require.EqualValues(t, 1, authEmptyCalls.Load())
		})
	}
	queries = nil
	recorder := httptest.NewRecorder()
	gateway.NewHandler(app, nil, "").ServeHTTP(recorder, httptest.NewRequest("GET", "/records?name=absent", nil))
	require.Equal(t, 200, recorder.Code)
	require.Len(t, queries, 1)
	require.False(t, strings.Contains(recorder.Body.String(), "Forbidden"))
	queries = nil
	recorder = httptest.NewRecorder()
	gateway.NewHandler(app, nil, "").ServeHTTP(recorder, httptest.NewRequest("GET", "/records/1?offset=10", nil))
	require.Equal(t, 200, recorder.Code, recorder.Body.String())
	require.Len(t, queries, 1)
	queries = nil
	result, err := native.CallTool(context.Background(), &schema.CallToolRequestParams{Name: "records.get", Arguments: map[string]any{"key": []int{1}, "offset": 10}})
	require.NoError(t, err)
	require.True(t, result.IsError == nil || !*result.IsError)
	require.Len(t, queries, 1)
	request := exec.ComponentRequest{Target: exec.ComponentTarget{Component: artifact.Component.Key, Route: spec.RouteRef{Method: "GET", Path: "/records/{key}"}}, Input: &input{Key: []int{2}, Tenant: 7, Limit: 10}}
	queries = nil
	sentinel := errors.New("probe dependency unavailable")
	failProbe = sentinel
	_, err = app.InvokeComponent(context.Background(), request)
	require.ErrorIs(t, err, sentinel)
	require.NotEqual(t, 403, response.ErrorStatusCode(err, 500))
	require.NotEqual(t, 404, response.ErrorStatusCode(err, 500))
	require.Len(t, queries, 2)
	queries = nil
	failProbe = nil
	canceled, cancel := context.WithCancel(context.Background())
	defer cancel()
	cancelProbe = cancel
	_, err = app.InvokeComponent(canceled, request)
	require.ErrorIs(t, err, context.Canceled)
	require.NotEqual(t, 403, response.ErrorStatusCode(err, 500))
	require.NotEqual(t, 404, response.ErrorStatusCode(err, 500))
	require.Len(t, queries, 2)
	cancelProbe = nil

	// The existing output tag opts out without introducing an output datapoint.
	type emptyOutput struct {
		Rows []row `json:"rows" onEmpty:"empty"`
	}
	offArtifact, err := bootstrap.BuildArtifact(bootstrap.ArtifactInput{Component: component, InputType: reflect.TypeFor[input](), OutputType: reflect.TypeFor[emptyOutput](), DirectViewField: "Rows", Types: types})
	require.NoError(t, err)
	offReader, err := sqlreader.NewExecution(sqlreader.Config{Component: offArtifact.Component, InputType: reflect.TypeFor[input](), OutputType: reflect.TypeFor[emptyOutput](), Plan: offArtifact.Reader, SQL: &dsql.SQLComponent{DB: db.DB}})
	require.NoError(t, err)
	queries = nil
	offRegistered := &registry.RegisteredComponent{Component: offArtifact.Component, Input: offArtifact.Input, OutputType: reflect.TypeFor[emptyOutput](), Reader: offReader.WithQueryGuard(func(_ context.Context, _ *sql.DB, q *xreader.ParameterizedQuery) error {
		queries = append(queries, q.Query)
		return nil
	})}
	offApp, err := druntime.NewRuntime([]*registry.RegisteredComponent{offRegistered})
	require.NoError(t, err)
	recorder = httptest.NewRecorder()
	gateway.NewHandler(offApp, nil, "").ServeHTTP(recorder, httptest.NewRequest("GET", "/records/2", nil))
	require.Equal(t, 200, recorder.Code, recorder.Body.String())
	require.Len(t, queries, 1)

}
