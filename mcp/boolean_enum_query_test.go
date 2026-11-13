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
	"github.com/viant/datly/spec"
	"github.com/viant/mcp-protocol/schema"
)

type booleanQueryInput struct {
	Flag int `parameter:",kind=query,in=flag,required=true" choice:"0:false" choice:"1:true" enumAccept:"boolean" enumNull:"reject"`
}

func TestBooleanEnumNativeQueryArgument(t *testing.T) {
	component := &spec.Component{Key: spec.Key{Kind: spec.KindComponent, Name: "boolean_query"}, Routes: []*spec.Route{{Method: "GET", Path: "/boolean-query", MCP: []*spec.MCPExposure{{Kind: spec.MCPExposureTool, Name: "boolean_query", CaseFormat: "lc"}}}}}
	artifact, err := bootstrap.BuildArtifact(bootstrap.ArtifactInput{Component: component, InputType: reflect.TypeFor[booleanQueryInput](), OutputType: reflect.TypeFor[booleanEnumRecord]()})
	require.NoError(t, err)
	calls, bound := 0, -1
	registered, err := artifact.Registration(druntime.RegisteredComponent{Handler: custom.NewFunc(func(_ context.Context, input *booleanQueryInput) (*booleanEnumRecord, error) {
		calls++
		bound = input.Flag
		return &booleanEnumRecord{Flag: input.Flag}, nil
	})})
	require.NoError(t, err)
	rt, err := druntime.NewRuntime([]*druntime.RegisteredComponent{registered})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, rt.Shutdown(context.Background())) })
	service, err := mcp.New(mcp.Config{Components: []*druntime.RegisteredComponent{registered}, Invoker: rt})
	require.NoError(t, err)
	require.NoError(t, service.PrepareTool(t.Context(), "boolean_query"))
	tool, found := service.Registry().ToolRegistry.Get("boolean_query")
	require.True(t, found)
	require.Equal(t, "boolean", tool.Metadata.InputSchema.Properties["flag"]["type"])
	for _, tc := range []struct {
		name  string
		value any
		valid bool
		want  int
	}{{"false", false, true, 0}, {"true", true, true, 1}, {"null", nil, false, 0}, {"ordinal", 1, false, 0}, {"literal", "true", false, 0}} {
		t.Run(tc.name, func(t *testing.T) {
			before := calls
			result, rpc := tool.Handler(t.Context(), &schema.CallToolRequest{Method: schema.MethodToolsCall, Params: schema.CallToolRequestParams{Name: "boolean_query", Arguments: map[string]any{"flag": tc.value}}})
			failed := rpc != nil || result == nil || result.IsError != nil && *result.IsError
			require.Equal(t, !tc.valid, failed, "%+v %v", result, rpc)
			if !tc.valid {
				require.Equal(t, before, calls)
				return
			}
			require.Equal(t, tc.want, bound)
			encoded, err := json.Marshal(result.StructuredContent)
			require.NoError(t, err)
			var body map[string]any
			require.NoError(t, json.Unmarshal(encoded, &body))
			require.Equal(t, tc.value, body["flag"])
		})
	}
}
