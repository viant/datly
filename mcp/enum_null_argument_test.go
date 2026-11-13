package mcp_test

import (
	"context"
	"encoding/json"
	"reflect"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/viant/datly/bootstrap"
	"github.com/viant/datly/exec"
	"github.com/viant/datly/mcp"
	druntime "github.com/viant/datly/runtime"
	"github.com/viant/datly/runtime/handler/custom"
	"github.com/viant/datly/runtime/registry"
	"github.com/viant/datly/spec"
	"github.com/viant/mcp-protocol/schema"
	"github.com/viant/tagly/choice"
)

type enumNullBody struct {
	Mode int `json:"mode" choice:"0:ASAP" choice:"1:EVEN" enumAccept:"literal" enumNull:"reject"`
}
type enumNullInput struct {
	Body *enumNullBody `parameter:"Body,kind=body,in=data"`
}
type enumNullRecorder struct {
	exec.ComponentInvoker
	err error
}

func (r *enumNullRecorder) InvokeComponent(ctx context.Context, request exec.ComponentRequest) (any, error) {
	result, err := r.ComponentInvoker.InvokeComponent(ctx, request)
	r.err = err
	return result, err
}
func TestMCPNestedEnumNullErrorIsActionable(t *testing.T) {
	component := &spec.Component{Key: spec.Key{Kind: spec.KindComponent, Name: "enum_null"}, Routes: []*spec.Route{{Method: "POST", Path: "/enum-null", MCP: []*spec.MCPExposure{{Kind: spec.MCPExposureTool, Name: "enum_null"}}}}}
	artifact, err := bootstrap.BuildArtifact(bootstrap.ArtifactInput{Component: component, InputType: reflect.TypeFor[enumNullInput](), OutputType: reflect.TypeFor[enumNullBody]()})
	require.NoError(t, err)
	calls := 0
	entry, err := artifact.Registration(registry.RegisteredComponent{Handler: custom.NewFunc(func(_ context.Context, in *enumNullInput) (*enumNullBody, error) { calls++; return in.Body, nil })})
	require.NoError(t, err)
	runtime, err := druntime.NewRuntime([]*registry.RegisteredComponent{entry})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, runtime.Shutdown(context.Background())) })
	recorder := &enumNullRecorder{ComponentInvoker: runtime}
	service, err := mcp.New(mcp.Config{Components: []*registry.RegisteredComponent{entry}, Invoker: recorder})
	require.NoError(t, err)
	tool, ok := service.Registry().ToolRegistry.Get("enum_null")
	require.True(t, ok)
	result, rpcErr := tool.Handler(t.Context(), &schema.CallToolRequest{Method: schema.MethodToolsCall, Params: schema.CallToolRequestParams{Name: "enum_null", Arguments: map[string]any{"body": map[string]any{"mode": nil}}}})
	require.Nil(t, rpcErr)
	require.NotNil(t, result)
	require.True(t, result.IsError != nil && *result.IsError)
	require.Zero(t, calls)
	require.ErrorIs(t, recorder.err, choice.ErrNullInput)
	raw, err := json.Marshal(result.StructuredContent)
	require.NoError(t, err)
	var body map[string]any
	require.NoError(t, json.Unmarshal(raw, &body))
	require.EqualValues(t, 400, body["status"])
	require.Equal(t, choice.ErrNullInput.Error(), body["message"])
}
