package mcp_test

import (
	"context"
	"encoding/json"
	"reflect"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/viant/datly/bootstrap"
	"github.com/viant/datly/mcp"
	druntime "github.com/viant/datly/runtime"
	"github.com/viant/datly/runtime/handler/custom"
	"github.com/viant/datly/runtime/registry"
	"github.com/viant/datly/spec"
	"github.com/viant/mcp-protocol/schema"
	legacy "github.com/viant/xdatly/handler/response"
)

type wrapperDeclaredOutput struct {
	legacy.Status
	Data []int `json:"data"`
}

func TestWrapperNativeMCPDeclaredErrorEnvelope(t *testing.T) {
	strict := false // Exercise native binding; strict schema admission is covered separately.
	component := &spec.Component{
		Key:        spec.Key{Kind: spec.KindComponent, Name: "wrapper_declared"},
		Routes:     []*spec.Route{{Method: "POST", Path: "/wrapper-declared", MCP: []*spec.MCPExposure{{Kind: spec.MCPExposureTool, Name: "wrapper_declared", StrictArguments: &strict}}}},
		Parameters: []*spec.Parameter{{Name: "Status", Source: spec.BindSource{Kind: "output", Name: "status"}}},
	}
	artifact, err := bootstrap.BuildArtifact(bootstrap.ArtifactInput{Component: component, InputType: reflect.TypeFor[wrapperTransportInput](), OutputType: reflect.TypeFor[wrapperDeclaredOutput]()})
	require.NoError(t, err)
	calls := 0
	entry, err := artifact.Registration(registry.RegisteredComponent{Handler: custom.NewFunc(func(_ context.Context, input *wrapperTransportInput) (*wrapperDeclaredOutput, error) {
		calls++
		return &wrapperDeclaredOutput{Data: input.Body}, nil
	})})
	require.NoError(t, err)
	runtime, err := druntime.NewRuntime([]*registry.RegisteredComponent{entry})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, runtime.Shutdown(context.Background())) })
	service, err := mcp.New(mcp.Config{Components: []*registry.RegisteredComponent{entry}, Invoker: runtime})
	require.NoError(t, err)
	tool, ok := service.Registry().ToolRegistry.Get("wrapper_declared")
	require.True(t, ok)
	for _, tc := range []struct{ name, raw, message string }{
		{"missing member", `{}`, "ids is required"},
		{"null member", `{"ids":null}`, "ids is required"},
		{"unknown member", `{"ids":[1],"extra":2}`, `json: unknown field "extra"`},
		{"opaque type failure", `{"ids":"PRIVATE-value"}`, "Bad Request"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var value any
			require.NoError(t, json.Unmarshal([]byte(tc.raw), &value))
			result, rpcErr := tool.Handler(t.Context(), &schema.CallToolRequest{Method: schema.MethodToolsCall, Params: schema.CallToolRequestParams{Name: "wrapper_declared", Arguments: map[string]any{"creativeIds": value}}})
			require.Nil(t, rpcErr)
			require.NotNil(t, result)
			require.NotNil(t, result.IsError)
			require.True(t, *result.IsError)
			require.Zero(t, calls, "invalid input must not invoke business code")
			wire, err := json.Marshal(result.StructuredContent)
			require.NoError(t, err)
			require.NotContains(t, string(wire), "PRIVATE")
			require.NotEmpty(t, result.Content)
			text, ok := result.Content[0].(schema.TextContent)
			require.True(t, ok)
			require.JSONEq(t, string(wire), text.Text)
			var envelope struct {
				Status, Message string
				Errors          []struct {
					Parameter, Message string
					StatusCode         int
				}
			}
			require.NoError(t, json.Unmarshal(wire, &envelope))
			require.Equal(t, "error", envelope.Status)
			require.Equal(t, tc.message, envelope.Message)
			require.Len(t, envelope.Errors, 1)
			require.Equal(t, "Body", envelope.Errors[0].Parameter)
			require.Equal(t, 400, envelope.Errors[0].StatusCode)
			require.Equal(t, tc.message, envelope.Errors[0].Message)
		})
	}
	result, rpcErr := tool.Handler(t.Context(), &schema.CallToolRequest{Method: schema.MethodToolsCall, Params: schema.CallToolRequestParams{Name: "wrapper_declared", Arguments: map[string]any{"creativeIds": map[string]any{"ids": []any{1}}}}})
	require.Nil(t, rpcErr)
	require.NotNil(t, result)
	require.False(t, result.IsError != nil && *result.IsError)
	require.Equal(t, 1, calls, "valid control must reach the native handler")
}
