package mcp_test

import (
	"context"
	"encoding/json"
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
			for _, tc := range []struct{ name, rows, metric, empty string }{{"GraphCamel", "childRows", "metricId", "emptyRows"}, {"GraphSnake", "child_rows", "metric_id", "empty_rows"}, {"GraphOriginal", "ChildRows", "MetricId", "EmptyRows"}, {"GraphCamel", "childRows", "metricId", "emptyRows"}} {
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
