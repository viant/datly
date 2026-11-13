package mcp_test

import (
	"context"
	"encoding/json"
	"net/http/httptest"
	"reflect"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/viant/datly/bootstrap"
	gateway "github.com/viant/datly/gateway/http"
	"github.com/viant/datly/mcp"
	mcpserver "github.com/viant/datly/mcp/server"
	druntime "github.com/viant/datly/runtime"
	"github.com/viant/datly/runtime/handler/custom"
	"github.com/viant/datly/runtime/registry"
	"github.com/viant/datly/spec"
	"github.com/viant/mcp-protocol/schema"
)

type nullAdmissionBody struct {
	OrderID *int    `json:"orderId" mcp:"name=orderId,aliases=legacyOrder" format:"nullable=false"`
	Note    *string `json:"note"`
}
type nullAdmissionInput struct {
	Body  *nullAdmissionBody `parameter:",kind=body" anonymous:"true"`
	Query *int               `parameter:",kind=query,in=query" json:"query" format:"nullable=false"`
}

func TestMCPAuthoredNullAdmissionAndREST(t *testing.T) {
	component := &spec.Component{Key: spec.Key{Kind: spec.KindComponent, Name: "null_admission"}, Routes: []*spec.Route{{Method: "POST", Path: "/null-admission", MCP: []*spec.MCPExposure{{Kind: spec.MCPExposureTool, Name: "null_admission"}}}}}
	artifact, err := bootstrap.BuildArtifact(bootstrap.ArtifactInput{Component: component, InputType: reflect.TypeFor[nullAdmissionInput](), OutputType: reflect.TypeFor[nullAdmissionBody]()})
	require.NoError(t, err)
	calls := 0
	entry, err := artifact.Registration(registry.RegisteredComponent{Handler: custom.NewFunc(func(_ context.Context, in *nullAdmissionInput) (*nullAdmissionBody, error) {
		calls++
		if in.Body == nil {
			return &nullAdmissionBody{}, nil
		}
		return in.Body, nil
	})})
	require.NoError(t, err)
	runtime, err := druntime.NewRuntime([]*registry.RegisteredComponent{entry})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, runtime.Shutdown(context.Background())) })
	service, err := mcp.New(mcp.Config{Components: []*registry.RegisteredComponent{entry}, Invoker: runtime})
	require.NoError(t, err)
	require.NoError(t, service.PrepareTool(t.Context(), "null_admission"))
	server, err := mcpserver.New(mcpserver.Config{Service: service, Transport: mcpserver.TransportConfig{Kind: mcpserver.TransportStreamable}})
	require.NoError(t, err)
	httpServer, err := server.HTTP()
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, server.Shutdown(context.Background())) })
	rpc := func(method string, arguments map[string]any) map[string]any {
		params := map[string]any{"_meta": map[string]any{"io.modelcontextprotocol/protocolVersion": schema.LatestProtocolVersion, "io.modelcontextprotocol/clientCapabilities": map[string]any{}}}
		if method == schema.MethodToolsCall {
			params["name"] = "null_admission"
			params["arguments"] = arguments
		}
		raw, err := json.Marshal(map[string]any{"jsonrpc": "2.0", "id": 1, "method": method, "params": params})
		require.NoError(t, err)
		request := httptest.NewRequest("POST", "/mcp", strings.NewReader(string(raw)))
		request.Header.Set("Content-Type", "application/json")
		request.Header.Set("Accept", "application/json, text/event-stream")
		request.Header.Set(schema.HeaderProtocolVersion, schema.LatestProtocolVersion)
		request.Header.Set(schema.HeaderMethod, method)
		if method == schema.MethodToolsCall {
			request.Header.Set(schema.HeaderName, "null_admission")
		}
		response := httptest.NewRecorder()
		httpServer.Handler.ServeHTTP(response, request)
		require.Equal(t, 200, response.Code, response.Body.String())
		data := response.Body.String()
		if strings.HasPrefix(response.Header().Get("Content-Type"), "text/event-stream") {
			var lines []string
			for _, line := range strings.Split(data, "\n") {
				if strings.HasPrefix(line, "data:") {
					lines = append(lines, strings.TrimSpace(strings.TrimPrefix(line, "data:")))
				}
			}
			data = strings.Join(lines, "\n")
		}
		var result map[string]any
		require.NoError(t, json.Unmarshal([]byte(data), &result))
		return result
	}
	listed := rpc(schema.MethodToolsList, nil)
	require.Nil(t, listed["error"])
	tools := listed["result"].(map[string]any)["tools"].([]any)
	require.Len(t, tools, 1)
	properties := tools[0].(map[string]any)["inputSchema"].(map[string]any)["properties"].(map[string]any)
	require.Equal(t, "integer", properties["orderId"].(map[string]any)["type"])
	for _, args := range []map[string]any{{"orderId": nil}, {"legacyOrder": nil}, {"query": nil}} {
		before := calls
		failed := rpc(schema.MethodToolsCall, args)
		require.Equal(t, float64(-32602), failed["error"].(map[string]any)["code"])
		require.Equal(t, before, calls)
	}
	require.Nil(t, rpc(schema.MethodToolsCall, map[string]any{})["error"])
	tool, ok := service.Registry().ToolRegistry.Get("null_admission")
	require.True(t, ok)
	require.Equal(t, "integer", tool.Metadata.InputSchema.Properties["orderId"]["type"])
	require.Equal(t, "integer", tool.Metadata.InputSchema.Properties["query"]["type"])
	for _, tc := range []struct {
		name  string
		args  map[string]any
		valid bool
	}{
		{"omitted", map[string]any{}, true},
		{"body null", map[string]any{"orderId": nil}, false},
		{"alias null", map[string]any{"legacyOrder": nil}, false},
		{"typed nil", map[string]any{"orderId": (*int)(nil)}, false},
		{"raw JSON null", map[string]any{"orderId": json.RawMessage(`null`)}, false},
		{"query null", map[string]any{"query": nil}, false},
		{"zero", map[string]any{"orderId": 0, "query": 0}, true},
		{"nullable unchanged", map[string]any{"note": nil}, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			before := calls
			result, rpcErr := tool.Handler(t.Context(), &schema.CallToolRequest{Method: schema.MethodToolsCall, Params: schema.CallToolRequestParams{Name: "null_admission", Arguments: tc.args}})
			if tc.valid {
				require.Nil(t, rpcErr)
				require.NotNil(t, result)
				require.Equal(t, before+1, calls)
			} else {
				require.NotNil(t, rpcErr)
				require.Equal(t, -32602, rpcErr.Code)
				require.Contains(t, rpcErr.Message, "cannot be null")
				require.Equal(t, before, calls)
			}
		})
	}
	response := httptest.NewRecorder()
	gateway.NewHandler(runtime, nil, "test").ServeHTTP(response, httptest.NewRequest("POST", "/null-admission", strings.NewReader(`{"orderId":null}`)))
	require.Equal(t, 200, response.Code, response.Body.String())
}
