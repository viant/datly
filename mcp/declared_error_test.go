package mcp

import (
	"context"
	"encoding/json"
	"errors"
	"reflect"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/viant/datly/exec"
	"github.com/viant/datly/runtime/registry"
	"github.com/viant/datly/spec"
	"github.com/viant/mcp-protocol/schema"
	legacy "github.com/viant/xdatly/handler/response"
)

type declaredFailureOutput struct {
	legacy.Status
	Data       []int `json:"data"`
	ExtraItems []string
	Private    string
}
type declaredFailureInvoker struct{}

func (*declaredFailureInvoker) InvokeComponent(context.Context, exec.ComponentRequest) (any, error) {
	return &declaredFailureOutput{Private: "must not leak"}, errors.New("private database detail")
}

func TestServiceDeclaredErrorUsesCompiledPresentation(t *testing.T) {
	for _, policy := range []string{"", "empty_array"} {
		t.Run(policy, func(t *testing.T) {
			entry := buildServiceComponent(t, serviceComponentFixture{name: "Records", inputType: reflect.TypeOf(struct{}{}), route: &spec.Route{Method: "GET", Path: "/records", MCP: []*spec.MCPExposure{{Kind: spec.MCPExposureTool, Name: "records"}}}})
			entry.OutputType = reflect.TypeFor[declaredFailureOutput]()
			entry.Component.Parameters = []*spec.Parameter{{Name: "Status", Source: spec.BindSource{Kind: "output", Name: "status"}}, {Name: "Data", Source: spec.BindSource{Kind: "output", Name: "view"}}}
			entry.Component.RootView = &spec.View{Name: "records", Source: &spec.ViewSource{SQL: "SELECT id FROM records"}}
			entry.Component.Settings = &spec.Settings{CaseFormat: "lc", Output: &spec.OutputSettings{NilSlicePolicy: policy, Exclude: []string{"Private"}}}
			service, err := New(Config{Components: []*registry.RegisteredComponent{entry}, Invoker: &declaredFailureInvoker{}})
			require.NoError(t, err)
			tool, ok := service.Registry().ToolRegistry.Get("records")
			require.True(t, ok)
			result, rpcErr := tool.Handler(context.Background(), &schema.CallToolRequest{Method: schema.MethodToolsCall, Params: schema.CallToolRequestParams{Name: "records"}})
			require.Nil(t, rpcErr)
			require.True(t, *result.IsError)
			text := result.Content[0].(schema.TextContent).Text
			structured, err := json.Marshal(result.StructuredContent)
			require.NoError(t, err)
			require.JSONEq(t, text, string(structured))
			var body map[string]any
			require.NoError(t, json.Unmarshal(structured, &body))
			require.Equal(t, "error", body["status"])
			require.Equal(t, "Internal Server Error", body["message"])
			require.Equal(t, []any{}, body["data"])
			require.NotContains(t, body, "Private")
			require.NotContains(t, body, "private")
			require.NotContains(t, text, "database detail")
			_, present := body["extraItems"]
			require.True(t, present)
			if policy == "empty_array" {
				require.Equal(t, []any{}, body["extraItems"])
			} else {
				require.Nil(t, body["extraItems"])
			}
		})
	}
}
