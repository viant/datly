package mcp_test

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http/httptest"
	"net/url"
	"reflect"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/viant/datly/bootstrap"
	gateway "github.com/viant/datly/gateway/http"
	"github.com/viant/datly/internal/testharness/sqlite"
	"github.com/viant/datly/mcp"
	druntime "github.com/viant/datly/runtime"
	"github.com/viant/datly/spec"
	dsql "github.com/viant/datly/sql"
	"github.com/viant/mcp-protocol/schema"
	"github.com/viant/sqlx/testutil/sqlfault"
)

type invocationSelectionRow struct {
	ID    int     `sqlx:"id" json:"id"`
	Label string  `sqlx:"label" json:"label"`
	Kind  string  `sqlx:"kind" internal:"true" json:"-"`
	CPM   float64 `sqlx:"-" json:"cpm"`
}

func (r *invocationSelectionRow) OnFetch(context.Context) error {
	if r.Kind == "banner" {
		r.CPM = 1.75
	}
	return nil
}

type invocationSelectionOutput struct {
	Data []*invocationSelectionRow `parameter:",kind=output,in=view" json:"data"`
}

func TestMCPOutputOnlySelectionPreservesComputationAndREST(t *testing.T) {
	ctx := t.Context()
	h := sqlite.New(t)
	require.NoError(t, h.ExecStatements(ctx, "CREATE TABLE selection_records(id INTEGER,kind TEXT,label TEXT)", "INSERT INTO selection_records VALUES(1,'banner','first'),(2,'banner','second')"))
	var queries atomic.Int32
	db := h.FaultDB(t, func(_ context.Context, call sqlfault.Call) error {
		if call.Phase == "query" && strings.Contains(call.SQL, "FROM selection_records") {
			queries.Add(1)
		}
		return nil
	})
	component := &spec.Component{Key: spec.Key{Kind: spec.KindComponent, Name: "SelectionComputation"}, Routes: []*spec.Route{{Method: "GET", Path: "/selection-computation", MCP: []*spec.MCPExposure{{Kind: spec.MCPExposureTool, Name: "SelectionComputation"}}}}, RootView: &spec.View{Name: "records", Selector: &spec.Selector{AllowFields: true}, Source: &spec.ViewSource{SQL: "SELECT id,kind,label FROM selection_records ORDER BY id"}}}
	started := time.Now()
	artifact, err := bootstrap.BuildArtifact(bootstrap.ArtifactInput{Component: component, InputType: reflect.TypeFor[caseInput](), OutputType: reflect.TypeFor[invocationSelectionOutput](), DirectViewField: "Data"})
	require.NoError(t, err)
	reader, err := artifact.ReaderCompilation().NewExecution(bootstrap.ReaderRuntimeConfig{SQL: &dsql.SQLComponent{DB: db}})
	require.NoError(t, err)
	registered, err := artifact.Registration(druntime.RegisteredComponent{Reader: reader})
	require.NoError(t, err)
	rt, err := druntime.NewRuntime([]*druntime.RegisteredComponent{registered})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, rt.Shutdown(context.Background())) })
	service, err := mcp.New(mcp.Config{Components: []*druntime.RegisteredComponent{registered}, Invoker: rt})
	require.NoError(t, err)
	require.NoError(t, service.PrepareTool(ctx, "SelectionComputation"))
	t.Logf("startup including compiled graph and discovery: %s", time.Since(started))
	tool, ok := service.Registry().ToolRegistry.Get("SelectionComputation")
	require.True(t, ok)
	handler := gateway.NewHandler(rt, nil, "test")
	for iteration := 0; iteration < 3; iteration++ {
		for _, selected := range []bool{false, true} {
			queries.Store(0)
			args := map[string]any{}
			expected := `{"data":[{"id":1,"cpm":1.75,"label":"first"},{"id":2,"cpm":1.75,"label":"second"}]}`
			if selected {
				args["fields"] = []string{"id", "cpm"}
				expected = `{"data":[{"id":1,"cpm":1.75},{"id":2,"cpm":1.75}]}`
			}
			start := time.Now()
			result, rpcErr := tool.Handler(ctx, &schema.CallToolRequest{Method: schema.MethodToolsCall, Params: schema.CallToolRequestParams{Name: "SelectionComputation", Arguments: args}})
			elapsed := time.Since(start)
			require.Nil(t, rpcErr)
			require.NotNil(t, result)
			if result.IsError != nil {
				require.False(t, *result.IsError)
			}
			encoded, err := json.Marshal(result.StructuredContent)
			require.NoError(t, err)
			require.JSONEq(t, expected, string(encoded))
			require.EqualValues(t, 1, queries.Load())
			t.Logf("iteration=%d selected=%t native MCP bind/read/compute/encode=%s queries=%d", iteration, selected, elapsed, queries.Load())
		}
		// The same compiled component still uses its static query projection for REST.
		queries.Store(0)
		response := httptest.NewRecorder()
		handler.ServeHTTP(response, httptest.NewRequest("GET", "/selection-computation?fields=id&fields=cpm", nil))
		require.Equal(t, 200, response.Code, response.Body.String())
		require.JSONEq(t, `{"data":[{"id":1,"cpm":0},{"id":2,"cpm":0}]}`, response.Body.String())
		require.EqualValues(t, 1, queries.Load())
	}
	// Concurrent transport calls share prepared metadata, never invocation mode.
	for i := 0; i < 12; i++ {
		t.Run(fmt.Sprintf("concurrent-%d", i), func(t *testing.T) {
			t.Parallel()
			if i%2 == 0 {
				response := httptest.NewRecorder()
				handler.ServeHTTP(response, httptest.NewRequest("GET", "/selection-computation?fields=id&fields=cpm", nil))
				require.Equal(t, 200, response.Code)
				require.JSONEq(t, `{"data":[{"id":1,"cpm":0},{"id":2,"cpm":0}]}`, response.Body.String())
				return
			}
			result, rpcErr := tool.Handler(t.Context(), &schema.CallToolRequest{Method: schema.MethodToolsCall, Params: schema.CallToolRequestParams{Name: "SelectionComputation", Arguments: map[string]any{"fields": []string{"id", "cpm"}}}})
			require.Nil(t, rpcErr)
			require.NotNil(t, result)
			encoded, err := json.Marshal(result.StructuredContent)
			require.NoError(t, err)
			require.JSONEq(t, `{"data":[{"id":1,"cpm":1.75},{"id":2,"cpm":1.75}]}`, string(encoded))
		})
	}

}

// BenchmarkMCPSelectionStartup measures complete immutable compilation, reader
// registration, runtime construction and MCP discovery for a 17-view graph.
// The no-fields policy is a preparation baseline: both graphs have identical
// sources and shapes; its fields are inadmissible and need no membership plan.
func BenchmarkMCPSelectionStartup(b *testing.B) {
	child := reflect.StructOf([]reflect.StructField{{Name: "ParentID", Type: reflect.TypeFor[int](), Tag: `sqlx:"parent_id"`}, {Name: "Value", Type: reflect.TypeFor[string](), Tag: `sqlx:"value"`}})
	fields := []reflect.StructField{{Name: "ID", Type: reflect.TypeFor[int](), Tag: `sqlx:"id"`}, {Name: "Kind", Type: reflect.TypeFor[string](), Tag: `sqlx:"kind"`}}
	for i := 0; i < 16; i++ {
		fields = append(fields, reflect.StructField{Name: fmt.Sprintf("Child%d", i), Type: reflect.SliceOf(reflect.PointerTo(child)), Tag: `sqlx:"-"`})
	}
	row := reflect.StructOf(fields)
	output := reflect.StructOf([]reflect.StructField{{Name: "Data", Type: reflect.SliceOf(reflect.PointerTo(row)), Tag: `parameter:",kind=output,in=view" json:"data"`}})
	for _, admitted := range []bool{false, true} {
		b.Run(fmt.Sprintf("fields-admitted-%t", admitted), func(b *testing.B) {
			component := &spec.Component{Key: spec.Key{Kind: spec.KindComponent, Name: "StartupSelection"}, Routes: []*spec.Route{{Method: "GET", Path: "/startup-selection", MCP: []*spec.MCPExposure{{Kind: spec.MCPExposureTool, Name: "StartupSelection"}}}}, RootView: &spec.View{Name: "root", Selector: &spec.Selector{AllowFields: admitted}, Source: &spec.ViewSource{SQL: "SELECT id,kind FROM records"}}}
			for i := 0; i < 16; i++ {
				component.RootView.Relations = append(component.RootView.Relations, &spec.Relation{Name: fmt.Sprintf("child%d", i), Holder: fmt.Sprintf("Child%d", i), Cardinality: spec.CardinalityMany, View: &spec.View{Name: fmt.Sprintf("child%d", i), Selector: &spec.Selector{AllowFields: admitted}, Source: &spec.ViewSource{SQL: "SELECT parent_id,value FROM children"}}, On: []*spec.RelationLink{{ParentColumn: "id", ChildColumn: "parent_id"}}})
			}
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				artifact, err := bootstrap.BuildArtifact(bootstrap.ArtifactInput{Component: component, InputType: reflect.TypeFor[struct{}](), OutputType: output, DirectViewField: "Data"})
				if err != nil {
					b.Fatal(err)
				}
				reader, err := artifact.ReaderCompilation().NewExecution(bootstrap.ReaderRuntimeConfig{SQL: &dsql.SQLComponent{}})
				if err != nil {
					b.Fatal(err)
				}
				registered, err := artifact.Registration(druntime.RegisteredComponent{Reader: reader})
				if err != nil {
					b.Fatal(err)
				}
				rt, err := druntime.NewRuntime([]*druntime.RegisteredComponent{registered})
				if err != nil {
					b.Fatal(err)
				}
				service, err := mcp.New(mcp.Config{Components: []*druntime.RegisteredComponent{registered}, Invoker: rt})
				if err != nil {
					b.Fatal(err)
				}
				if err = service.PrepareTool(b.Context(), "StartupSelection"); err != nil {
					b.Fatal(err)
				}
				if err = rt.Shutdown(context.Background()); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

type groupedSelectionRow struct {
	ID    int    `sqlx:"id" json:"id"`
	Kind  string `sqlx:"kind" json:"kind"`
	Total int    `sqlx:"total" json:"total"`
}
type groupedSelectionOutput struct {
	Data []*groupedSelectionRow `parameter:",kind=output,in=view" json:"data"`
}

func TestMCPGroupedFieldsRetainQueryGrain(t *testing.T) {
	h := sqlite.New(t)
	require.NoError(t, h.ExecStatements(t.Context(), "CREATE TABLE grouped_selection(id INTEGER,kind TEXT,amount INTEGER)", "INSERT INTO grouped_selection VALUES(1,'a',10),(1,'a',5),(1,'b',20)"))
	grouped := true
	component := &spec.Component{Key: spec.Key{Kind: spec.KindComponent, Name: "GroupedSelection"}, Routes: []*spec.Route{{Method: "GET", Path: "/grouped-selection", MCP: []*spec.MCPExposure{{Kind: spec.MCPExposureTool, Name: "GroupedSelection"}}}}, RootView: &spec.View{Name: "records", Groupable: &grouped, Selector: &spec.Selector{AllowFields: true}, Source: &spec.ViewSource{SQL: "SELECT id,kind,SUM(amount) AS total FROM grouped_selection GROUP BY id,kind"}}}
	artifact, err := bootstrap.BuildArtifact(bootstrap.ArtifactInput{Component: component, InputType: reflect.TypeFor[caseInput](), OutputType: reflect.TypeFor[groupedSelectionOutput](), DirectViewField: "Data"})
	require.NoError(t, err)
	reader, err := artifact.ReaderCompilation().NewExecution(bootstrap.ReaderRuntimeConfig{SQL: &dsql.SQLComponent{DB: h.DB}})
	require.NoError(t, err)
	registered, err := artifact.Registration(druntime.RegisteredComponent{Reader: reader})
	require.NoError(t, err)
	rt, err := druntime.NewRuntime([]*druntime.RegisteredComponent{registered})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, rt.Shutdown(context.Background())) })
	service, err := mcp.New(mcp.Config{Components: []*druntime.RegisteredComponent{registered}, Invoker: rt})
	require.NoError(t, err)
	require.NoError(t, service.PrepareTool(t.Context(), "GroupedSelection"))
	tool, ok := service.Registry().ToolRegistry.Get("GroupedSelection")
	require.True(t, ok)
	for _, fields := range [][]string{{"id", "total"}, {"total"}} {
		result, rpcErr := tool.Handler(t.Context(), &schema.CallToolRequest{Method: schema.MethodToolsCall, Params: schema.CallToolRequestParams{Name: "GroupedSelection", Arguments: map[string]any{"fields": fields}}})
		require.Nil(t, rpcErr)
		require.NotNil(t, result)
		expected := `{"data":[{"id":1,"total":35}]}`
		if len(fields) == 1 {
			expected = `{"data":[{"total":35}]}`
		}
		encoded, err := json.Marshal(result.StructuredContent)
		require.NoError(t, err)
		require.JSONEq(t, expected, string(encoded), "grouped Fields retain native dimension selection and aggregate grain")
		response := httptest.NewRecorder()
		gateway.NewHandler(rt, nil, "test").ServeHTTP(response, httptest.NewRequest("GET", "/grouped-selection?"+url.Values{"fields": fields}.Encode(), nil))
		require.Equal(t, 200, response.Code, response.Body.String())
		require.JSONEq(t, expected, response.Body.String())
	}
}
