package mcp_test

import (
	"context"
	"encoding/json"
	"github.com/stretchr/testify/require"
	"github.com/viant/bindly"
	"github.com/viant/datly/bootstrap"
	"github.com/viant/datly/internal/testharness/mcpclient"
	"github.com/viant/datly/mcp"
	druntime "github.com/viant/datly/runtime"
	"github.com/viant/datly/runtime/handler/custom"
	"github.com/viant/datly/runtime/registry"
	"github.com/viant/datly/spec"
	"github.com/viant/mcp-protocol/schema"
	xcontract "github.com/viant/xdatly/contract"
	"reflect"
	"testing"
)

type explicitContractContent struct {
	Type string `json:"type"`
	Text string `json:"text"`
}
type explicitContractData struct {
	Public string `json:"public"`
	Secret string `json:"secret"`
}
type explicitContractOutput struct {
	Content           []explicitContractContent `json:"content"`
	StructuredContent *explicitContractData     `json:"structuredContent"`
	ResultType        string                    `json:"resultType"`
}

func (*explicitContractOutput) MCPToolResult() {}
func (*explicitContractOutput) Contract(context.Context, bindly.Binder) (*xcontract.Policy, error) {
	return &xcontract.Policy{Name: "explicit-result-restricted", Field: func(_ string, parent reflect.Type, field *xcontract.Field) (*xcontract.Field, error) {
		if parent == reflect.TypeFor[explicitContractData]() && field.Name == "Secret" {
			return field.Clone().Hide(), nil
		}
		return nil, nil
	}}, nil
}

func TestExplicitToolResultPreservesNativeDynamicOutputContract(t *testing.T) {
	component := &spec.Component{Key: spec.Key{Kind: spec.KindComponent, Name: "ExplicitContent"}, Routes: []*spec.Route{{Method: "POST", Path: "/explicit-content", MCP: []*spec.MCPExposure{{Kind: spec.MCPExposureTool, Name: "explicit_content"}}}}}
	artifact, err := bootstrap.BuildArtifact(bootstrap.ArtifactInput{Component: component, InputType: reflect.TypeFor[struct{}](), OutputType: reflect.TypeFor[explicitContractOutput]()})
	require.NoError(t, err)
	registered, err := artifact.Registration(registry.RegisteredComponent{Handler: custom.NewFunc(func(context.Context, *struct{}) (*explicitContractOutput, error) {
		return &explicitContractOutput{Content: []explicitContractContent{}, StructuredContent: &explicitContractData{Public: "visible", Secret: "PRIVATE raw output"}, ResultType: "complete"}, nil
	})})
	require.NoError(t, err)
	runtime, err := druntime.NewRuntime([]*registry.RegisteredComponent{registered})
	require.NoError(t, err)
	t.Cleanup(func() { runtime.Shutdown(context.Background()) })
	service, err := mcp.New(mcp.Config{Components: []*registry.RegisteredComponent{registered}, Invoker: runtime})
	require.NoError(t, err)
	client := mcpclient.New(t, service, schema.LatestProtocolVersion)
	listed, err := client.ListTools(t.Context(), nil)
	require.NoError(t, err)
	require.Len(t, listed.Tools, 1)
	require.Nil(t, listed.Tools[0].OutputSchema)
	result, err := client.CallTool(t.Context(), &schema.CallToolRequestParams{Name: "explicit_content", Arguments: map[string]any{}})
	require.NoError(t, err)
	require.False(t, result.IsError != nil && *result.IsError)
	wire, err := json.Marshal(result)
	require.NoError(t, err)
	require.NotContains(t, string(wire), "PRIVATE")
	require.NotContains(t, string(wire), "secret")
	require.Contains(t, string(wire), "visible")
	require.Contains(t, string(wire), `"public":"visible"`)
}
