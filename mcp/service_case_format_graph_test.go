package mcp_test

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http/httptest"
	"reflect"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/viant/datly/bootstrap"
	gateway "github.com/viant/datly/gateway/http"
	"github.com/viant/datly/mcp"
	druntime "github.com/viant/datly/runtime"
	"github.com/viant/datly/runtime/handler/custom"
	"github.com/viant/datly/spec"
	"github.com/viant/mcp-protocol/schema"
	"github.com/xeipuuv/gojsonschema"
)

type graphCaseRow struct {
	MetricID int
	Fixed    string `json:"Exact_Name"`
	Secret   string
}
type graphCaseOutput struct {
	ChildRows []graphCaseRow
	EmptyRows []graphCaseRow
}

// Distinct exposures must never change the shared HTTP output or each other's plan.
func TestMCPExposureCaseFormatOutputGraphIsolation(t *testing.T) {
	for _, indexed := range []bool{false, true} {
		t.Run(map[bool]string{false: "eager", true: "indexed"}[indexed], func(t *testing.T) {
			ctx := context.Background()
			component := &spec.Component{Key: spec.Key{Kind: spec.KindComponent, Name: "GraphCase"}, Settings: &spec.Settings{CaseFormat: "uc", Output: &spec.OutputSettings{Exclude: []string{"ChildRows.Secret"}, NilSlicePolicy: "empty_array"}}, Routes: []*spec.Route{{Method: "GET", Path: "/graph-case", MCP: []*spec.MCPExposure{{Kind: spec.MCPExposureTool, Name: "GraphCamel", CaseFormat: "lc"}, {Kind: spec.MCPExposureTool, Name: "GraphSnake", CaseFormat: "lu"}, {Kind: spec.MCPExposureTool, Name: "GraphOriginal"}}}}}
			artifact, err := bootstrap.BuildArtifact(bootstrap.ArtifactInput{Component: component, InputType: reflect.TypeFor[struct{}](), OutputType: reflect.TypeFor[graphCaseOutput]()})
			require.NoError(t, err)
			registered, err := artifact.Registration(druntime.RegisteredComponent{Handler: custom.NewFunc[struct{}, graphCaseOutput](func(context.Context, *struct{}) (*graphCaseOutput, error) {
				return &graphCaseOutput{ChildRows: []graphCaseRow{{MetricID: 7, Fixed: "fixed", Secret: "hidden"}}}, nil
			})})
			require.NoError(t, err)
			config := mcp.Config{Components: []*druntime.RegisteredComponent{registered}}
			var rt *druntime.Runtime
			if indexed {
				rt, err = druntime.NewIndexedRuntime([]*spec.Component{artifact.Component}, nil, caseLoader{registered})
				config.Components = nil
				config.Indexed = []*spec.Component{artifact.Component}
				config.Loader = rt
			} else {
				rt, err = druntime.NewRuntime(config.Components)
			}
			require.NoError(t, err)
			t.Cleanup(func() { require.NoError(t, rt.Shutdown(ctx)) })
			config.Invoker = rt
			service, err := mcp.New(config)
			require.NoError(t, err)
			for _, tc := range []struct{ name, rows, metric, empty string }{{"GraphCamel", "childRows", "metricId", "emptyRows"}, {"GraphSnake", "child_rows", "metric_id", "empty_rows"}, {"GraphOriginal", "childRows", "metricId", "emptyRows"}, {"GraphCamel", "childRows", "metricId", "emptyRows"}} {
				require.NoError(t, service.PrepareTool(ctx, tc.name))
				tool, ok := service.Registry().ToolRegistry.Get(tc.name)
				require.True(t, ok)
				result, rpcErr := tool.Handler(ctx, &schema.CallToolRequest{Method: schema.MethodToolsCall, Params: schema.CallToolRequestParams{Name: tc.name}})
				require.Nil(t, rpcErr)
				actual, err := json.Marshal(result.StructuredContent)
				require.NoError(t, err)
				expected := map[string]any{tc.rows: []any{map[string]any{tc.metric: 7, "Exact_Name": "fixed"}}, tc.empty: []any{}}
				expectedBytes, err := json.Marshal(expected)
				require.NoError(t, err)
				require.JSONEq(t, string(expectedBytes), string(actual))
				schemaBytes, err := json.Marshal(tool.Metadata.OutputSchema)
				require.NoError(t, err)
				validation, err := gojsonschema.Validate(gojsonschema.NewBytesLoader(schemaBytes), gojsonschema.NewBytesLoader(actual))
				require.NoError(t, err)
				require.True(t, validation.Valid(), "%v", validation.Errors())
			}
			res := httptest.NewRecorder()
			gateway.NewHandler(rt, nil, "test").ServeHTTP(res, httptest.NewRequest("GET", "/graph-case", nil))
			require.Equal(t, 200, res.Code, res.Body.String())
			require.Contains(t, res.Body.String(), `"ChildRows"`)
			require.NotContains(t, res.Body.String(), `"childRows"`)
			require.Equal(t, "uc", component.Settings.CaseFormat)
		})
	}
}

type resourceGraphInput struct {
	ID int `parameter:",kind=query,in=id"`
}

func TestMCPResourceCaseFormatOutputGraphIsolation(t *testing.T) {
	for _, template := range []bool{false, true} {
		for _, tc := range []struct{ format, rows, metric string }{{"", "childRows", "metricId"}, {"lu", "child_rows", "metric_id"}, {"uc", "ChildRows", "MetricId"}} {
			t.Run(fmt.Sprintf("template=%v/format=%s", template, tc.format), func(t *testing.T) {
				ctx := context.Background()
				kind := spec.MCPExposureResource
				handler := custom.NewFunc[struct{}, graphCaseOutput](func(context.Context, *struct{}) (*graphCaseOutput, error) {
					return &graphCaseOutput{ChildRows: []graphCaseRow{{MetricID: 7, Fixed: "fixed"}}}, nil
				})
				inputType := handler.InputType()
				uri := "datly://localhost/resource-case"
				if template {
					kind = spec.MCPExposureResourceTemplate
					handler = custom.NewFunc[resourceGraphInput, graphCaseOutput](func(context.Context, *resourceGraphInput) (*graphCaseOutput, error) {
						return &graphCaseOutput{ChildRows: []graphCaseRow{{MetricID: 7, Fixed: "fixed"}}}, nil
					})
					inputType = handler.InputType()
					uri += "?id=1"
				}
				component := &spec.Component{Key: spec.Key{Kind: spec.KindComponent, Name: "ResourceCase"}, Settings: &spec.Settings{CaseFormat: "uc"}, Routes: []*spec.Route{{Method: "GET", Path: "/resource-case", MCP: []*spec.MCPExposure{{Kind: kind, Name: "ResourceCase", CaseFormat: tc.format}}}}}
				artifact, err := bootstrap.BuildArtifact(bootstrap.ArtifactInput{Component: component, InputType: inputType, OutputType: reflect.TypeFor[graphCaseOutput]()})
				require.NoError(t, err)
				registered, err := artifact.Registration(druntime.RegisteredComponent{Handler: handler})
				require.NoError(t, err)
				rt, err := druntime.NewRuntime([]*druntime.RegisteredComponent{registered})
				require.NoError(t, err)
				t.Cleanup(func() { require.NoError(t, rt.Shutdown(ctx)) })
				service, err := mcp.New(mcp.Config{Components: []*druntime.RegisteredComponent{registered}, Invoker: rt})
				require.NoError(t, err)
				result, rpcErr := service.ReadResource(ctx, &schema.ReadResourceRequest{Method: schema.MethodResourcesRead, Params: schema.ReadResourceRequestParams{Uri: uri}})
				require.Nil(t, rpcErr)
				require.Len(t, result.Contents, 1)
				var body map[string]any
				require.NoError(t, json.Unmarshal([]byte(result.Contents[0].Text), &body))
				require.Contains(t, body, tc.rows)
				rows := body[tc.rows].([]any)
				require.Equal(t, float64(7), rows[0].(map[string]any)[tc.metric])
				require.Equal(t, "fixed", rows[0].(map[string]any)["Exact_Name"])
				require.Equal(t, "uc", component.Settings.CaseFormat)
			})
		}
	}
}
