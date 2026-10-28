package mcp_test

import (
	"context"
	"reflect"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/viant/datly/bootstrap"
	"github.com/viant/datly/mcp"
	druntime "github.com/viant/datly/runtime"
	"github.com/viant/datly/runtime/handler/custom"
	"github.com/viant/datly/spec"
	"github.com/viant/mcp-protocol/schema"
)

func TestMCPDefaultInputNamesPreserveLegacyDespiteHTTPCaseFormat(t *testing.T) {
	type useCase struct{ description, httpFormat string }
	for _, tc := range []useCase{{"lower camel HTTP", "lc"}, {"snake HTTP", "lu"}, {"dash HTTP", "ld"}} {
		t.Run(tc.description, func(t *testing.T) {
			component := &spec.Component{Key: spec.Key{Kind: spec.KindComponent, Name: "LegacyNames"}, Settings: &spec.Settings{CaseFormat: tc.httpFormat}, Routes: []*spec.Route{{Method: "POST", Path: "/legacy-names", MCP: []*spec.MCPExposure{{Kind: spec.MCPExposureTool, Name: "LegacyNames"}}}}}
			artifact, err := bootstrap.BuildArtifact(bootstrap.ArtifactInput{Component: component, InputType: reflect.TypeFor[inputCaseRequest](), OutputType: reflect.TypeFor[inputCaseResult]()})
			require.NoError(t, err)
			var bound *inputCaseBody
			registered, err := artifact.Registration(druntime.RegisteredComponent{Handler: custom.NewFunc[inputCaseRequest, inputCaseResult](func(_ context.Context, in *inputCaseRequest) (*inputCaseResult, error) {
				bound = in.Body
				return &inputCaseResult{}, nil
			})})
			require.NoError(t, err)
			rt, err := druntime.NewRuntime([]*druntime.RegisteredComponent{registered})
			require.NoError(t, err)
			t.Cleanup(func() { require.NoError(t, rt.Shutdown(context.Background())) })
			service, err := mcp.New(mcp.Config{Components: []*druntime.RegisteredComponent{registered}, Invoker: rt})
			require.NoError(t, err)
			require.NoError(t, service.PrepareTool(context.Background(), "LegacyNames"))
			tool, ok := service.Registry().ToolRegistry.Get("LegacyNames")
			require.True(t, ok)
			props := tool.Metadata.InputSchema.Properties
			for _, name := range []string{"Id", "Name", "AutoAdvertising", "Children", "Exact_Name", "AccountingId"} {
				require.Contains(t, props, name)
			}
			require.Contains(t, props["Children"]["items"].(map[string]any)["properties"], "MetricID")
			require.NotContains(t, props, "Has")
			require.Contains(t, tool.Metadata.InputSchema.Required, "Id")
			result, rpcErr := tool.Handler(context.Background(), &schema.CallToolRequest{Method: schema.MethodToolsCall, Params: schema.CallToolRequestParams{Name: "LegacyNames", Arguments: map[string]any{"Id": 7, "Name": "legacy", "AutoAdvertising": false, "Children": []any{map[string]any{"MetricID": 11}}, "Exact_Name": "explicit", "AccountingId": 9}}})
			require.Nil(t, rpcErr)
			require.False(t, result.IsError != nil && *result.IsError)
			require.Equal(t, &inputCaseBody{Id: 7, Name: ptrLegacyName("legacy"), AutoAdvertising: ptrLegacyName(false), Children: []inputCaseChild{{MetricID: 11}}, Explicit: "explicit", Named: 9}, bound)
			require.Equal(t, tc.httpFormat, component.Settings.CaseFormat)
		})
	}
}

func ptrLegacyName[T any](value T) *T { return &value }
