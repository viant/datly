package mcp

import (
	"context"
	"database/sql"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"reflect"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/viant/datly/bootstrap"
	gateway "github.com/viant/datly/gateway/http"
	"github.com/viant/datly/internal/testharness"
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
	legacy "github.com/viant/xdatly/handler/response"
	xreader "github.com/viant/xdatly/reader"
)

// Native identity classification must preserve the declared reader envelope,
// just as other execution failures do, without weakening explicit public bodies.
func TestReaderAuthEmptyDeclaredEnvelope(t *testing.T) {
	type input struct {
		Key    []int `parameter:"Key,kind=path,in=key,uri=/envelope/{key}" predicate:"in,r,id" json:"key"`
		Tenant int   `parameter:"Tenant,kind=query,in=tenant,value=7" predicate:"handler,example.com/auth.Scope,auth=true" json:"tenant"`
	}
	type row struct {
		ID int `sqlx:"id,primaryKey=true" json:"id"`
	}
	type output struct {
		legacy.Status `parameter:",kind=output,in=status"`
		Data          []row `parameter:",kind=output,in=view" view:"Records" json:"data" forbiddenNotFoundCheck:"true"`
	}
	db := sqlite.New(t)
	require.NoError(t, db.ExecStatements(context.Background(), "CREATE TABLE records(id INTEGER, tenant INTEGER)", "INSERT INTO records VALUES(1,7),(2,8)"))
	types := typecatalog.NewCatalog()
	require.NoError(t, types.Register(typecatalog.TypeOriginPackage, &x.Type{Type: reflect.TypeFor[authEmptyPredicate](), PkgPath: "example.com/auth", Name: "Scope"}))
	c := &spec.Component{Key: spec.Key{Kind: spec.KindComponent, Name: "AuthEmptyEnvelope"}, RootView: &spec.View{Name: "Records", Columns: []*spec.Column{{Name: "ID", Source: "id", PrimaryKey: true}}, Source: &spec.ViewSource{SQL: `SELECT r.id FROM records r WHERE 1=1 ${predicate.Builder().CombineAnd($predicate.FilterGroup(0,"AND")).Build("AND")}`}}, Routes: []*spec.Route{{Method: "GET", Path: "/envelope/{key}", MCP: []*spec.MCPExposure{{Kind: spec.MCPExposureTool, Name: "envelope.get"}}}}}
	a, err := bootstrap.BuildArtifact(bootstrap.ArtifactInput{Component: c, InputType: reflect.TypeFor[input](), OutputType: reflect.TypeFor[output](), DirectViewField: "Data", Types: types})
	require.NoError(t, err)
	r, err := sqlreader.NewExecution(sqlreader.Config{Component: a.Component, InputType: reflect.TypeFor[input](), OutputType: reflect.TypeFor[output](), Plan: a.Reader, SQL: &dsql.SQLComponent{DB: db.DB}})
	require.NoError(t, err)
	queries := 0
	guarded := r.WithQueryGuard(func(context.Context, *sql.DB, *xreader.ParameterizedQuery) error { queries++; return nil })
	registered := &registry.RegisteredComponent{Component: a.Component, Input: a.Input, OutputType: reflect.TypeFor[output](), Reader: guarded}
	app, err := druntime.NewRuntime([]*registry.RegisteredComponent{registered})
	require.NoError(t, err)
	service, err := New(Config{Components: []*registry.RegisteredComponent{registered}, Invoker: app})
	require.NoError(t, err)
	client := mcpclient.New(t, service, schema.LatestProtocolVersion)
	for _, tc := range []struct {
		name, key  string
		id, status int
	}{{"denied", "2", 2, 403}, {"missing", "99", 99, 404}} {
		t.Run(tc.name, func(t *testing.T) {
			t.Run("HTTP", func(t *testing.T) {
				queries = 0
				authEmptyCalls.Store(0)
				recorder := httptest.NewRecorder()
				gateway.NewHandler(app, nil, "").ServeHTTP(recorder, httptest.NewRequest("GET", "/envelope/"+tc.key, nil))
				require.Equal(t, tc.status, recorder.Code)
				require.Contains(t, recorder.Body.String(), `"data":[]`)
				var body map[string]any
				require.NoError(t, json.Unmarshal(recorder.Body.Bytes(), &body))
				require.Equal(t, "error", body["status"])
				require.Equal(t, http.StatusText(tc.status), body["message"])
				require.Equal(t, 2, queries)
				require.EqualValues(t, 1, authEmptyCalls.Load())
			})
			t.Run("MCP", func(t *testing.T) {
				queries = 0
				authEmptyCalls.Store(0)
				result, err := client.CallTool(context.Background(), &schema.CallToolRequestParams{Name: "envelope.get", Arguments: map[string]any{"key": []int{tc.id}}})
				require.NoError(t, err)
				require.NotNil(t, result.IsError)
				require.True(t, *result.IsError)
				code, found := schema.ToolErrorHTTPStatus(result)
				require.True(t, found)
				require.Equal(t, tc.status, code)
				body := testharness.StructuredObject(t, result.StructuredContent)
				require.Equal(t, "error", body["status"])
				require.Equal(t, []any{}, body["data"])
				require.Equal(t, http.StatusText(tc.status), body["message"])
				require.Equal(t, 2, queries)
				require.EqualValues(t, 1, authEmptyCalls.Load())
			})
		})
	}
}
