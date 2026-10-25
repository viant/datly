package report

import (
	"context"
	"encoding/json"
	"net/http/httptest"
	"reflect"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/viant/datly/bootstrap"
	httpgateway "github.com/viant/datly/gateway/http"
	"github.com/viant/datly/internal/testharness"
	"github.com/viant/datly/mcp"
	druntime "github.com/viant/datly/runtime"
	"github.com/viant/datly/spec"
	dsql "github.com/viant/datly/sql"
	"github.com/viant/datly/typecatalog"
	"github.com/viant/mcp-protocol/schema"
)

type wordBoundaryInput struct {
	VStart, V100, ViewConversions, AccountID, IsPG, URL int
	V25Threshold, V100Window, ControlVExposed           int
	AccountIDs                                          []int
}

type wordBoundaryRow struct {
	VStart          int `sqlx:"v_start" json:"VStart"`
	V100            int `sqlx:"v100" json:"V100" groupable:"true"`
	ViewConversions int `sqlx:"view_conversions" json:"readerConversions"`
	AccountID       int `sqlx:"account_id" json:"AccountID" groupable:"true"`
	IsPG            int `sqlx:"is_pg" json:"IsPG" groupable:"true"`
	URL             int `sqlx:"url" json:"URL" groupable:"true"`
	V25Threshold    int `sqlx:"v25_threshold" json:"V25Threshold" groupable:"true"`
	V100Window      int `sqlx:"v100_window" json:"readerWindow"`
	ControlVExposed int `sqlx:"control_v_exposed" json:"ControlVExposed" groupable:"true"`
}

type wordBoundaryOutput struct {
	Rows []*wordBoundaryRow `parameter:",kind=output,in=view" json:"data"`
}

func TestDynamicCubeWordBoundariesHTTPMCP(t *testing.T) {
	ctx := context.Background()
	db := testharness.NewSQLiteHarness(t)
	require.NoError(t, db.ExecStatements(ctx,
		"CREATE TABLE metrics(v_start INTEGER,v100 INTEGER,view_conversions INTEGER,account_id INTEGER,is_pg INTEGER,url INTEGER,v25_threshold INTEGER,v100_window INTEGER,control_v_exposed INTEGER)",
		"INSERT INTO metrics VALUES(2,100,3,7,1,9,11,12,13)"))
	component := &spec.Component{
		Key:      spec.Key{Kind: spec.KindComponent, Scope: "example.com/reporting", Name: "Metrics"},
		Settings: &spec.Settings{Report: &spec.ReportSettings{Enabled: true}},
		Routes:   []*spec.Route{{Method: "GET", Path: "/metrics"}},
		RootView: &spec.View{Name: "metrics", Groupable: boolPointer(true), Selector: &spec.Selector{AllowFields: true},
			Source: &spec.ViewSource{SQL: "SELECT v100,account_id,is_pg,url,v25_threshold,control_v_exposed,SUM(v_start) AS v_start,SUM(view_conversions) AS view_conversions,SUM(v100_window) AS v100_window FROM metrics GROUP BY v100,account_id,is_pg,url,v25_threshold,control_v_exposed"}},
	}
	names := []string{"vStart", "v100", "viewConversions", "accountId", "isPg", "url", "v25Threshold", "v100Window", "controlVExposed"}
	sqlNames := []string{"v_start", "v100", "view_conversions", "account_id", "is_pg", "url", "v25_threshold", "v100_window", "control_v_exposed"}
	for i, wire := range names {
		field := reflect.TypeFor[wordBoundaryRow]().Field(i)
		component.RootView.Columns = append(component.RootView.Columns, &spec.Column{Name: field.Name, Source: sqlNames[i], Tag: string(field.Tag), Groupable: boolPointer(i != 0 && i != 2 && i != 7)})
		component.Parameters = append(component.Parameters, &spec.Parameter{Name: field.Name, TypeExpr: "int", Source: spec.BindSource{Kind: "query", Name: wire}, Required: boolPointer(false)})
	}
	component.Parameters = append(component.Parameters, &spec.Parameter{Name: "AccountIDs", TypeExpr: "[]int", Source: spec.BindSource{Kind: "query", Name: "account_ids"}, Required: boolPointer(false)})
	before := component.Clone()
	compilation, err := NewProjectCompiler(ProjectConfig{Types: typecatalog.NewCatalog()}).CompileArtifacts([]bootstrap.ArtifactInput{{
		Component: component, InputType: reflect.TypeFor[wordBoundaryInput](), OutputType: reflect.TypeFor[wordBoundaryOutput](),
	}})
	require.NoError(t, err)
	require.Equal(t, before, component.Clone())
	registered, err := compilation.RuntimeComponents(ctx, RuntimeConfigureFunc(func(_ context.Context, artifact *ComponentArtifact) (RuntimeCapabilities, error) {
		if artifact.IsReport() {
			return RuntimeCapabilities{}, nil
		}
		reader, err := artifact.ReaderCompilation().NewExecution(bootstrap.ReaderRuntimeConfig{SQL: &dsql.SQLComponent{DB: db.DB}})
		return RuntimeCapabilities{Reader: reader}, err
	}))
	require.NoError(t, err)
	runtime, err := druntime.NewRuntime(registered)
	require.NoError(t, err)
	t.Cleanup(func() { runtime.Shutdown(ctx) })
	const body = `{"dimensions":{"v100":true,"accountId":true,"isPg":true,"url":true,"v25Threshold":true,"controlVExposed":true},"measures":{"vStart":true,"viewConversions":true,"v100Window":true},"filters":{"vStart":2,"v100":100,"viewConversions":3,"accountId":7,"isPg":1,"url":9,"v25Threshold":11,"v100Window":12,"controlVExposed":13,"accountIds":[7,8]}}`
	for _, artifact := range compilation.Artifacts() {
		if !artifact.IsReport() {
			continue
		}
		input := reflect.New(artifact.InputType())
		require.NoError(t, json.Unmarshal([]byte(body), input.Interface()))
		for i, wire := range names {
			name := reflect.TypeFor[wordBoundaryRow]().Field(i).Name
			section := "Dimensions"
			if i == 0 || i == 2 || i == 7 {
				section = "Measures"
			}
			selection, _ := artifact.InputType().FieldByName(section)
			field, ok := selection.Type.FieldByName(name)
			require.True(t, ok)
			require.Equal(t, wire+",omitempty", field.Tag.Get("json"))
			require.True(t, input.Elem().FieldByName(section).FieldByName(name).Bool())
			filter := input.Elem().FieldByName("Filters").FieldByName(name)
			require.False(t, filter.IsNil(), name)
		}
		accounts := input.Elem().FieldByName("Filters").FieldByName("AccountIDs")
		require.False(t, accounts.IsNil())
		require.Equal(t, []int{7, 8}, accounts.Elem().Interface())
	}
	service, err := mcp.New(mcp.Config{Components: registered, Invoker: runtime})
	require.NoError(t, err)
	tool, ok := service.Catalog().Tool("MetricsCube")
	require.True(t, ok)
	require.NotContains(t, tool.Metadata().InputSchema.Properties["measures"]["properties"], "vsTart")
	require.NotContains(t, tool.Metadata().InputSchema.Properties["filters"]["properties"], "vsTart")
	require.Contains(t, tool.Metadata().InputSchema.Properties["filters"]["properties"], "accountIds")
	for _, invalid := range []string{"v25tHreshold", "v100wIndow", "controlVeXposed", "accountIdS"} {
		for _, section := range []string{"dimensions", "measures", "filters"} {
			require.NotContains(t, tool.Metadata().InputSchema.Properties[section]["properties"], invalid)
		}
	}
	for i, wire := range names {
		section := "dimensions"
		if i == 0 || i == 2 || i == 7 {
			section = "measures"
		}
		require.Contains(t, tool.Metadata().InputSchema.Properties[section]["properties"], wire)
		require.Contains(t, tool.Metadata().InputSchema.Properties["filters"]["properties"], wire)
	}
	const expected = `{"data":[{"VStart":2,"V100":100,"readerConversions":3,"AccountID":7,"IsPG":1,"URL":9,"V25Threshold":11,"readerWindow":12,"ControlVExposed":13}]}`
	for _, request := range []struct{ method, path, body, expected string }{
		{"GET", "/metrics", "", expected},
		{"POST", "/metrics/cube", body, expected},
		{"POST", "/metrics/cube", `{"measures":{"vStart":true}}`, `{"data":[{"VStart":2}]}`},
	} {
		req := httptest.NewRequest(request.method, request.path, strings.NewReader(request.body))
		req.Header.Set("Content-Type", "application/json")
		response := httptest.NewRecorder()
		httpgateway.NewHandler(runtime, nil, "").ServeHTTP(response, req)
		require.Equal(t, 200, response.Code, response.Body.String())
		require.JSONEq(t, request.expected, response.Body.String())
	}
	cube, ok := service.Registry().ToolRegistry.Get("MetricsCube")
	require.True(t, ok)
	var arguments map[string]any
	require.NoError(t, json.Unmarshal([]byte(body), &arguments))
	result, protocolErr := cube.Handler(ctx, &schema.CallToolRequest{Method: schema.MethodToolsCall, Params: schema.CallToolRequestParams{Name: "MetricsCube", Arguments: arguments}})
	require.Nil(t, protocolErr)
	require.NotNil(t, result)
	require.False(t, result.IsError != nil && *result.IsError)
	require.JSONEq(t, expected, result.Content[0].(schema.TextContent).Text)
}
