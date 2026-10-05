package mcp

import (
	"context"
	"errors"
	"reflect"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/viant/datly/exec"
	"github.com/viant/datly/internal/testharness"
	"github.com/viant/datly/runtime/registry"
	"github.com/viant/datly/spec"
	"github.com/viant/mcp-protocol/schema"
	"github.com/viant/xdatly/response"
)

type policyErrorBody struct {
	Items []string `json:"items"`
}
type unrelatedPolicyErrorBody struct {
	Items []string `json:"items"`
}
type publicBodyInvoker struct{ body any }

func (i *publicBodyInvoker) InvokeComponent(context.Context, exec.ComponentRequest) (any, error) {
	// Unrelated success value must never supply error presentation authority.
	return map[string]any{"secret": true}, &response.Error{Code: 400, Payload: i.body, Cause: errors.New("private")}
}

func TestServiceNilSlicePolicyExplicitErrorPresentation(t *testing.T) {
	for _, policy := range []string{"", "null", "empty_array"} {
		for _, tc := range []struct {
			name     string
			body     any
			matching bool
		}{
			{"value", policyErrorBody{}, true}, {"pointer", &policyErrorBody{}, true}, {"nilPointer", (*policyErrorBody)(nil), true},
			{"unrelated", unrelatedPolicyErrorBody{}, false}, {"map", map[string]any{"items": []string(nil)}, false},
		} {
			t.Run(policy+"/"+tc.name, func(t *testing.T) {
				component := buildServiceComponent(t, serviceComponentFixture{name: "Policy", inputType: reflect.TypeOf(struct{}{}), route: &spec.Route{Method: "POST", Path: "/policy", MCP: []*spec.MCPExposure{{Kind: spec.MCPExposureTool, Name: "policy"}}}})
				component.OutputType = reflect.TypeFor[policyErrorBody]()
				component.Component.Settings = &spec.Settings{Output: &spec.OutputSettings{NilSlicePolicy: policy}}
				service, err := New(Config{Components: []*registry.RegisteredComponent{component}, Invoker: &publicBodyInvoker{body: tc.body}})
				require.NoError(t, err)
				entry, ok := service.Registry().ToolRegistry.Get("policy")
				require.True(t, ok)
				result, protocolErr := entry.Handler(context.Background(), &schema.CallToolRequest{Method: schema.MethodToolsCall, Params: schema.CallToolRequestParams{Name: "policy"}})
				require.Nil(t, protocolErr)
				require.NotNil(t, result.IsError)
				require.True(t, *result.IsError)
				text := result.Content[0].(schema.TextContent).Text
				if tc.name == "nilPointer" {
					require.Equal(t, "null", text)
					require.Nil(t, result.StructuredContent)
					return
				}
				object := testharness.StructuredObject(t, result.StructuredContent)
				if policy == "empty_array" && tc.matching {
					require.Equal(t, []any{}, object["items"])
					require.JSONEq(t, `{"items":[]}`, text)
				} else {
					require.Nil(t, object["items"])
					require.Equal(t, `{"items":null}`, text)
				}
				require.NotContains(t, object, "secret")
			})
		}
	}
}
