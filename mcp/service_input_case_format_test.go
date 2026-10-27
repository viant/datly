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
	"github.com/viant/xdatly/response"
)

type inputCaseChild struct{ MetricID int }

type inputCaseBody struct {
	Children        []inputCaseChild
	Id              int `validate:"required"`
	Name            *string
	AutoAdvertising *bool
	Explicit        string    `json:"Exact_Name"`
	Named           int       `mcp:"AccountingId"`
	Has             *struct{} `json:"-" mcp:"-"`
}
type inputCaseRequest struct {
	Body *inputCaseBody `parameter:",kind=body,required=true" anonymous:"true"`
}
type inputCaseResult struct{ response.Status }

func TestMCPInputSchemaUsesComponentCaseFormat(t *testing.T) {
	type useCase struct{ description, format, id, name, flag, children, metric string }
	for _, tc := range []useCase{{"existing Go naming without format", "", "Id", "Name", "AutoAdvertising", "Children", "MetricID"}, {"lower camel component contract", "lc", "id", "name", "autoAdvertising", "children", "metricId"}, {"snake component contract", "lu", "id", "name", "auto_advertising", "children", "metric_id"}, {"dash component contract", "ld", "id", "name", "auto-advertising", "children", "metric-id"}} {
		t.Run(tc.description, func(t *testing.T) {
			ctx := context.Background()
			component := &spec.Component{Key: spec.Key{Kind: spec.KindComponent, Name: "InputCase"}, Settings: &spec.Settings{CaseFormat: tc.format}, Routes: []*spec.Route{{Method: "POST", Path: "/input-case", MCP: []*spec.MCPExposure{{Kind: spec.MCPExposureTool, Name: "InputCase"}}}}}
			artifact, err := bootstrap.BuildArtifact(bootstrap.ArtifactInput{Component: component, InputType: reflect.TypeFor[inputCaseRequest](), OutputType: reflect.TypeFor[inputCaseResult]()})
			require.NoError(t, err)
			var bound *inputCaseBody
			registered, err := artifact.Registration(druntime.RegisteredComponent{Handler: custom.NewFunc[inputCaseRequest, inputCaseResult](func(_ context.Context, in *inputCaseRequest) (*inputCaseResult, error) {
				bound = in.Body
				return &inputCaseResult{Status: response.Status{Status: "ok"}}, nil
			})})
			require.NoError(t, err)
			rt, err := druntime.NewRuntime([]*druntime.RegisteredComponent{registered})
			require.NoError(t, err)
			t.Cleanup(func() { require.NoError(t, rt.Shutdown(context.Background())) })
			service, err := mcp.New(mcp.Config{Components: []*druntime.RegisteredComponent{registered}, Invoker: rt})
			require.NoError(t, err)
			require.NoError(t, service.PrepareTool(ctx, "InputCase"))
			tool, found := service.Registry().ToolRegistry.Get("InputCase")
			require.True(t, found)
			props := tool.Metadata.InputSchema.Properties
			for _, name := range []string{tc.id, tc.name, tc.flag, tc.children, "Exact_Name", "AccountingId"} {
				require.Contains(t, props, name)
			}
			items := props[tc.children]["items"].(map[string]any)
			require.Contains(t, items["properties"], tc.metric)
			require.NotContains(t, props, "Has")
			require.NotContains(t, props, "has")
			require.Contains(t, tool.Metadata.InputSchema.Required, tc.id)
			require.Contains(t, tool.Metadata.InputSchema.Required, "AccountingId")
			result, rpcErr := tool.Handler(ctx, &schema.CallToolRequest{Method: schema.MethodToolsCall, Params: schema.CallToolRequestParams{Name: "InputCase", Arguments: map[string]any{tc.id: float64(7), tc.name: "updated", tc.flag: false, tc.children: []any{map[string]any{tc.metric: float64(11)}}, "Exact_Name": "fixed", "AccountingId": float64(9)}}})
			require.Nil(t, rpcErr)
			require.False(t, result.IsError != nil && *result.IsError)
			require.NotNil(t, bound)
			require.Equal(t, 7, bound.Id)
			require.Equal(t, "updated", *bound.Name)
			require.NotNil(t, bound.AutoAdvertising)
			require.False(t, *bound.AutoAdvertising)
			require.Equal(t, "fixed", bound.Explicit)
			require.Equal(t, 9, bound.Named)
			require.Equal(t, []inputCaseChild{{MetricID: 11}}, bound.Children)
		})
	}
}
