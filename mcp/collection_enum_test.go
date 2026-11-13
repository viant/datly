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
	druntime "github.com/viant/datly/runtime"
	"github.com/viant/datly/runtime/handler/custom"
	"github.com/viant/datly/runtime/registry"
	"github.com/viant/datly/spec"
	"github.com/viant/mcp-protocol/schema"
)

type collectionEnumBody struct {
	ChannelsV2 []string  `json:"channelsV2" mcpEnum:"true" choice:"DISPLAY" choice:"VIDEO"`
	Optional   *[]string `json:"optional" mcpEnum:"true" choice:"DISPLAY" choice:"VIDEO"`
}
type collectionEnumInput struct {
	Body   *collectionEnumBody `parameter:",kind=body" anonymous:"true"`
	Named  []string            `parameter:",kind=body,in=named" mcpEnum:"true" choice:"DISPLAY" choice:"VIDEO"`
	NoNull *[]string           `parameter:",kind=body,in=noNull" mcpEnum:"true" choice:"DISPLAY" choice:"VIDEO" format:"nullable=false"`
}

func TestMCPCollectionEnumThroughNativeCallAndREST(t *testing.T) {
	component := &spec.Component{Key: spec.Key{Kind: spec.KindComponent, Name: "collection_enum"}, Routes: []*spec.Route{{Method: "POST", Path: "/collection-enum", MCP: []*spec.MCPExposure{{Kind: spec.MCPExposureTool, Name: "collection_enum"}}}}}
	artifact, err := bootstrap.BuildArtifact(bootstrap.ArtifactInput{Component: component, InputType: reflect.TypeFor[collectionEnumInput](), OutputType: reflect.TypeFor[collectionEnumBody]()})
	require.NoError(t, err)
	calls := 0
	entry, err := artifact.Registration(registry.RegisteredComponent{Handler: custom.NewFunc(func(_ context.Context, in *collectionEnumInput) (*collectionEnumBody, error) {
		calls++
		return in.Body, nil
	})})
	require.NoError(t, err)
	runtime, err := druntime.NewRuntime([]*registry.RegisteredComponent{entry})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, runtime.Shutdown(context.Background())) })
	service, err := mcp.New(mcp.Config{Components: []*registry.RegisteredComponent{entry}, Invoker: runtime})
	require.NoError(t, err)
	require.NoError(t, service.PrepareTool(t.Context(), "collection_enum"))
	tool, ok := service.Registry().ToolRegistry.Get("collection_enum")
	require.True(t, ok)
	for _, name := range []string{"channelsV2", "optional", "named"} {
		property := tool.Metadata.InputSchema.Properties[name]
		require.NotNil(t, property, name)
		require.Equal(t, []string{"array", "null"}, property["type"], name)
		require.Equal(t, []any{"DISPLAY", "VIDEO"}, property["items"].(map[string]any)["enum"], name)
	}
	require.NotNil(t, tool.Metadata.OutputSchema)
	require.Equal(t, []interface{}{"array", "null"}, tool.Metadata.OutputSchema.Properties["channelsV2"]["type"])
	require.Equal(t, "array", tool.Metadata.InputSchema.Properties["noNull"]["type"])
	require.Equal(t, []any{"DISPLAY", "VIDEO"}, tool.Metadata.OutputSchema.Properties["channelsV2"]["items"].(map[string]any)["enum"])
	for _, tc := range []struct {
		name  string
		args  map[string]any
		valid bool
	}{
		{"valid", map[string]any{"channelsV2": []any{"DISPLAY", "VIDEO"}, "optional": []any{"DISPLAY"}, "named": []any{"VIDEO"}}, true},
		{"unknown", map[string]any{"channelsV2": []any{"UNKNOWN"}}, false},
		{"named unknown", map[string]any{"named": []any{"UNKNOWN"}}, false},
		{"pointer unknown", map[string]any{"optional": []any{"UNKNOWN"}}, false},
		{"explicit nonnull", map[string]any{"noNull": nil}, false},
		{"explicit nonnull valid", map[string]any{"noNull": []any{"DISPLAY"}}, true},
		{"null item", map[string]any{"channelsV2": []any{nil}}, false},
		{"number item", map[string]any{"channelsV2": []any{1}}, false},
		{"null", map[string]any{"channelsV2": nil, "optional": nil, "named": nil}, true},
		{"empty", map[string]any{"channelsV2": []any{}, "optional": []any{}, "named": []any{}}, true},
		{"omitted", map[string]any{}, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			before := calls
			result, rpcErr := tool.Handler(t.Context(), &schema.CallToolRequest{Method: schema.MethodToolsCall, Params: schema.CallToolRequestParams{Name: "collection_enum", Arguments: tc.args}})
			if tc.valid {
				require.Nil(t, rpcErr)
				require.NotNil(t, result)
				require.False(t, result.IsError != nil && *result.IsError, "%+v", result)
				require.Equal(t, before+1, calls)
			} else {
				require.NotNil(t, rpcErr)
				require.Equal(t, -32602, rpcErr.Code)
				if tc.name == "explicit nonnull" {
					require.Contains(t, rpcErr.Message, "cannot be null")
				} else {
					require.Contains(t, rpcErr.Message, "enum collection")
				}
				require.Equal(t, before, calls)
			}
			if tc.name == "valid" {
				raw, err := json.Marshal(result.StructuredContent)
				require.NoError(t, err)
				require.JSONEq(t, `{"channelsV2":["DISPLAY","VIDEO"],"optional":["DISPLAY"]}`, string(raw))
			}
		})
	}
	response := httptest.NewRecorder()
	gateway.NewHandler(runtime, nil, "test").ServeHTTP(response, httptest.NewRequest("POST", "/collection-enum", strings.NewReader(`{"channelsV2":["UNKNOWN"],"optional":["OTHER"]}`)))
	require.Equal(t, 200, response.Code, response.Body.String())
	require.JSONEq(t, `{"channelsV2":["UNKNOWN"],"optional":["OTHER"]}`, response.Body.String())
}
