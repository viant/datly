package mcp_test

import (
	"context"
	"encoding/json"
	gateway "github.com/viant/datly/gateway/http"
	"net/http/httptest"
	"reflect"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/viant/datly/bootstrap"
	"github.com/viant/datly/exec"
	"github.com/viant/datly/mcp"
	druntime "github.com/viant/datly/runtime"
	"github.com/viant/datly/runtime/handler/custom"
	"github.com/viant/datly/spec"
	"github.com/viant/mcp-protocol/schema"
)

type collapsedPathInput struct {
	ID   *int    `parameter:"ID,kind=path,in=id,uri=/{id}" pathMcpEnabled:"false"`
	Name *string `parameter:"Name,kind=query,in=name"`
}
type collapsedPathOutput struct {
	ID   *int    `json:"id"`
	Name *string `json:"name"`
}

func TestMCPHiddenPathVariantAcceptsOptionalIdentityOnCollectionTool(t *testing.T) {
	component := &spec.Component{Key: spec.Key{Kind: spec.KindComponent, Name: "Things"}, Routes: []*spec.Route{{Method: "GET", Path: "/things", MCP: []*spec.MCPExposure{{Kind: spec.MCPExposureTool, Name: "get_things"}}}}}
	artifact, err := bootstrap.BuildArtifact(bootstrap.ArtifactInput{Component: component, InputType: reflect.TypeFor[collapsedPathInput](), OutputType: reflect.TypeFor[collapsedPathOutput]()})
	require.NoError(t, err)
	var received *collapsedPathInput
	entry, err := artifact.Registration(druntime.RegisteredComponent{Handler: custom.NewFunc(func(_ context.Context, in *collapsedPathInput) (*collapsedPathOutput, error) {
		received = in
		return &collapsedPathOutput{ID: in.ID, Name: in.Name}, nil
	})})
	require.NoError(t, err)
	rt, err := druntime.NewRuntime([]*druntime.RegisteredComponent{entry})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, rt.Shutdown(context.Background())) })
	authorizedRoutes := []string{}
	service, err := mcp.New(mcp.Config{AuthorizeTool: func(_ context.Context, target exec.ComponentTarget) error {
		authorizedRoutes = append(authorizedRoutes, target.Route.Path)
		return nil
	}, Components: []*druntime.RegisteredComponent{entry}, Invoker: rt, StrictArguments: true})
	require.NoError(t, err)
	require.Equal(t, []string{"get_things"}, service.Catalog().ToolNames())
	tool, ok := service.Registry().ToolRegistry.Get("get_things")
	require.True(t, ok)
	require.Contains(t, tool.Metadata.InputSchema.Properties, "id")
	require.NotContains(t, tool.Metadata.InputSchema.Required, "id")
	discovered, _, err := service.DescribeTool(t.Context(), tool.Metadata)
	require.NoError(t, err)
	require.Contains(t, discovered.InputSchema.Properties, "id")
	require.NotContains(t, discovered.InputSchema.Required, "id")

	for _, tc := range []struct {
		args map[string]any
		id   *int
		name *string
	}{
		{map[string]any{}, nil, nil},
		{map[string]any{"id": 42}, ptrLegacyName(42), nil},
		{map[string]any{"id": 42, "name": "example"}, ptrLegacyName(42), ptrLegacyName("example")},
		{map[string]any{"name": "example"}, nil, ptrLegacyName("example")},
	} {
		result, rpcErr := tool.Handler(t.Context(), &schema.CallToolRequest{Method: schema.MethodToolsCall, Params: schema.CallToolRequestParams{Name: "get_things", Arguments: tc.args}})
		require.Nil(t, rpcErr)
		require.NotNil(t, result)
		require.False(t, result.IsError != nil && *result.IsError)
		require.Equal(t, tc.id, received.ID)
		require.Equal(t, tc.name, received.Name)
		expectedRoute := "/things"
		if tc.id != nil {
			expectedRoute = "/things/{id}"
		}
		require.Equal(t, expectedRoute, authorizedRoutes[len(authorizedRoutes)-1])
	}
	for _, args := range []map[string]any{{"id": nil}, {"id": ""}, {"id": []int{}}} {
		received = nil
		_, rpcErr := tool.Handler(t.Context(), &schema.CallToolRequest{Method: schema.MethodToolsCall, Params: schema.CallToolRequestParams{Name: "get_things", Arguments: args}})
		require.NotNil(t, rpcErr)
		require.Nil(t, received, "invalid identity must not fall back to an unfiltered read")
	}
	for _, tc := range []struct {
		path string
		id   *int
		name *string
	}{
		{"/things?name=example", nil, ptrLegacyName("example")},
		{"/things/42?name=example", ptrLegacyName(42), ptrLegacyName("example")},
		{"/things?id=42", nil, nil},
	} {
		rec := httptest.NewRecorder()
		gateway.NewHandler(rt, nil, "test").ServeHTTP(rec, httptest.NewRequest("GET", tc.path, nil))
		require.Equal(t, 200, rec.Code, rec.Body.String())
		require.Equal(t, tc.id, received.ID)
		require.Equal(t, tc.name, received.Name)
	}

}

type collapsedBatchInput struct {
	ID   []int   `parameter:"ID,kind=path,in=id,uri=/{id}" pathMcpEnabled:"false" mcp:"name=id,aliases=ids"`
	Name *string `parameter:"Name,kind=query,in=name"`
}
type collapsedBatchOutput struct {
	Data []int `json:"data"`
}

func TestMCPCollapsedPathPublishesBatchIdentityAndAliases(t *testing.T) {
	component := &spec.Component{Key: spec.Key{Kind: spec.KindComponent, Name: "BatchThings"}, Routes: []*spec.Route{{Method: "GET", Path: "/batch-things", MCP: []*spec.MCPExposure{{Kind: spec.MCPExposureTool, Name: "get_batch_things"}}}}}
	artifact, err := bootstrap.BuildArtifact(bootstrap.ArtifactInput{Component: component, InputType: reflect.TypeFor[collapsedBatchInput](), OutputType: reflect.TypeFor[collapsedBatchOutput]()})
	require.NoError(t, err)
	calls := 0
	registered, err := artifact.Registration(druntime.RegisteredComponent{Handler: custom.NewFunc(func(_ context.Context, in *collapsedBatchInput) (*collapsedBatchOutput, error) {
		calls++
		return &collapsedBatchOutput{Data: in.ID}, nil
	})})
	require.NoError(t, err)
	rt, err := druntime.NewRuntime([]*druntime.RegisteredComponent{registered})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, rt.Shutdown(context.Background())) })
	service, err := mcp.New(mcp.Config{Components: []*druntime.RegisteredComponent{registered}, Invoker: rt, StrictArguments: true})
	require.NoError(t, err)
	entry, ok := service.Registry().ToolRegistry.Get("get_batch_things")
	require.True(t, ok)
	metadata, _, err := service.DescribeTool(t.Context(), entry.Metadata)
	require.NoError(t, err)
	require.Equal(t, "array", metadata.InputSchema.Properties["id"]["type"])
	require.NotContains(t, metadata.InputSchema.Required, "id")
	require.Equal(t, 0, calls, "discovery cannot execute the business reader")
	for _, key := range []string{"id", "ids"} {
		result, rpcErr := entry.Handler(t.Context(), &schema.CallToolRequest{Method: schema.MethodToolsCall, Params: schema.CallToolRequestParams{Name: "get_batch_things", Arguments: map[string]any{key: []any{float64(31), float64(32)}}}})
		require.Nil(t, rpcErr)
		require.False(t, result.IsError != nil && *result.IsError)
		encoded, err := json.Marshal(result.StructuredContent)
		require.NoError(t, err)
		require.JSONEq(t, `{"data":[31,32]}`, string(encoded))
	}
	before := calls
	_, rpcErr := entry.Handler(t.Context(), &schema.CallToolRequest{Method: schema.MethodToolsCall, Params: schema.CallToolRequestParams{Name: "get_batch_things", Arguments: map[string]any{"id": []int{31}, "ids": []int{32}}}})
	require.NotNil(t, rpcErr)
	require.Equal(t, before, calls, "conflicting aliases cannot execute")
}
