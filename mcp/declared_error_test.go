package mcp

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"testing"
	"time"

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

type canceledDeclaredFailureInvoker struct {
	failure error
	invoked func(context.Context)
}

func (i canceledDeclaredFailureInvoker) InvokeComponent(ctx context.Context, _ exec.ComponentRequest) (any, error) {
	i.invoked(ctx)
	return &declaredFailureOutput{Private: "must not leak"}, i.failure
}

func TestServiceDeclaredErrorRetainsEnvelopeOnRequestCancellation(t *testing.T) {
	for _, tc := range []struct {
		name     string
		deadline bool
		custom   bool
		related  bool
	}{
		{"canceled request", false, false, true},
		{"expired request", true, false, true},
		{"custom cancel cause", false, true, true},
		{"unrelated failure in canceled request", false, false, false},
		{"unrelated failure in custom canceled request", false, true, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx, cancel := context.WithCancel(t.Context())
			if tc.deadline {
				cancel()
				ctx, cancel = context.WithDeadline(t.Context(), time.Now().Add(-time.Second))
			}
			if tc.custom {
				cancel()
				customCtx, cancelWithCause := context.WithCancelCause(t.Context())
				cancelWithCause(errors.New("private cancellation cause"))
				ctx = customCtx
			} else {
				cancel()
			}
			failure := errors.New("private execution failure")
			if tc.related {
				failure = fmt.Errorf("private execution failure: %w", context.Cause(ctx))
			}
			invocations := 0
			invoker := canceledDeclaredFailureInvoker{failure: failure, invoked: func(actual context.Context) {
				invocations++
				require.ErrorIs(t, actual.Err(), ctx.Err(), "execution retains cancellation")
			}}
			entry := buildServiceComponent(t, serviceComponentFixture{name: "Records", inputType: reflect.TypeOf(struct{}{}), route: &spec.Route{Method: "GET", Path: "/records", MCP: []*spec.MCPExposure{{Kind: spec.MCPExposureTool, Name: "records"}}}})
			entry.OutputType = reflect.TypeFor[declaredFailureOutput]()
			entry.Component.Parameters = []*spec.Parameter{{Name: "Status", Source: spec.BindSource{Kind: "output", Name: "status"}}, {Name: "Data", Source: spec.BindSource{Kind: "output", Name: "view"}}}
			entry.Component.RootView = &spec.View{Name: "records", Source: &spec.ViewSource{SQL: "SELECT id FROM records"}}
			entry.Component.Settings = &spec.Settings{CaseFormat: "lc", Output: &spec.OutputSettings{NilSlicePolicy: "empty_array", Exclude: []string{"Private"}}}
			service, err := New(Config{Components: []*registry.RegisteredComponent{entry}, Invoker: invoker})
			require.NoError(t, err)
			tool, ok := service.Registry().ToolRegistry.Get("records")
			require.True(t, ok)
			result, rpcErr := tool.Handler(ctx, &schema.CallToolRequest{Method: schema.MethodToolsCall, Params: schema.CallToolRequestParams{Name: "records"}})
			require.Nil(t, rpcErr)
			require.Equal(t, 1, invocations)
			require.True(t, *result.IsError)
			raw, err := json.Marshal(result)
			require.NoError(t, err)
			require.NotContains(t, string(raw), "private execution failure")
			require.NotContains(t, string(raw), "must not leak")
			require.NotContains(t, string(raw), "private cancellation cause")
			body, err := json.Marshal(result.StructuredContent)
			require.NoError(t, err)
			text := result.Content[0].(schema.TextContent).Text
			require.JSONEq(t, string(body), text)
			code, found := schema.ToolErrorHTTPStatus(result)
			if !tc.related {
				require.False(t, found, "unrelated failure keeps the canceled generic fallback")
				require.JSONEq(t, `{"status":500,"error":true,"message":"Internal Server Error"}`, string(body))
				return
			}
			require.True(t, found, "missing typed status in %s", raw)
			require.Equal(t, 500, code)
			var envelope map[string]any
			require.NoError(t, json.Unmarshal(body, &envelope))
			require.Equal(t, "error", envelope["status"])
			require.Equal(t, "Internal Server Error", envelope["message"])
			require.Equal(t, []any{}, envelope["data"])
			require.Equal(t, []any{}, envelope["extraItems"])
		})
	}
}
