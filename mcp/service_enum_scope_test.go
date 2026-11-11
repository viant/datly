package mcp_test

import (
	"context"
	"encoding/json"
	"net/http/httptest"
	"reflect"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/viant/bindly"
	"github.com/viant/datly/bootstrap"
	gateway "github.com/viant/datly/gateway/http"
	"github.com/viant/datly/mcp"
	druntime "github.com/viant/datly/runtime"
	"github.com/viant/datly/runtime/handler/custom"
	"github.com/viant/datly/spec"
	"github.com/viant/mcp-protocol/schema"
	xcontract "github.com/viant/xdatly/contract"
	xmcp "github.com/viant/xdatly/handler/mcp"
)

type scopedEnumRecord struct {
	Code int `json:"code" choice:"0:ASAP" choice:"1:SPEND_EVENLY" enumAccept:"literal" enumStyle:"literal"`
}

func (*scopedEnumRecord) Contract(ctx context.Context, _ bindly.Binder) (*xcontract.Policy, error) {
	if _, isMCP := xmcp.LookupContext(ctx); isMCP {
		return nil, nil
	}
	return &xcontract.Policy{Name: "http-storage:v1", Field: func(_ string, parent reflect.Type, field *xcontract.Field) (*xcontract.Field, error) {
		if parent == reflect.TypeFor[scopedEnumRecord]() && field.Name == "Code" {
			return field.RemoveTag("choice").RemoveTag("enumAccept").RemoveTag("enumStyle"), nil
		}
		return nil, nil
	}}, nil
}

type scopedEnumInput struct {
	Body *scopedEnumRecord `parameter:",kind=body,in=data"`
}
type scopedEnumOutput struct {
	Data *scopedEnumRecord `json:"data"`
}

func TestMCPEnumsCanPreserveHTTPStorageRepresentation(t *testing.T) {
	component := &spec.Component{Key: spec.Key{Kind: spec.KindComponent, Name: "ScopedEnum"}, Routes: []*spec.Route{{Method: "POST", Path: "/scoped-enum", MCP: []*spec.MCPExposure{{Kind: spec.MCPExposureTool, Name: "scoped_enum", CaseFormat: "lc"}}}}}
	artifact, err := bootstrap.BuildArtifact(bootstrap.ArtifactInput{Component: component, InputType: reflect.TypeFor[scopedEnumInput](), OutputType: reflect.TypeFor[scopedEnumOutput]()})
	require.NoError(t, err)
	calls := 0
	registered, err := artifact.Registration(druntime.RegisteredComponent{Handler: custom.NewFunc[scopedEnumInput, scopedEnumOutput](func(_ context.Context, input *scopedEnumInput) (*scopedEnumOutput, error) {
		calls++
		require.Equal(t, 1, input.Body.Code)
		return &scopedEnumOutput{Data: input.Body}, nil
	})})
	require.NoError(t, err)
	rt, err := druntime.NewRuntime([]*druntime.RegisteredComponent{registered})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, rt.Shutdown(context.Background())) })
	service, err := mcp.New(mcp.Config{Components: []*druntime.RegisteredComponent{registered}, Invoker: rt})
	require.NoError(t, err)
	entry, present := service.Registry().ToolRegistry.Get("scoped_enum")
	require.True(t, present)
	for range 2 {
		metadata, _, err := service.DescribeTool(t.Context(), entry.Metadata)
		require.NoError(t, err)
		input := metadata.InputSchema.Properties["body"]["properties"].(map[string]any)["code"].(map[string]any)
		require.Equal(t, "string", input["type"])
		result, protocolErr := entry.Handler(t.Context(), &schema.CallToolRequest{Method: schema.MethodToolsCall, Params: schema.CallToolRequestParams{Name: "scoped_enum", Arguments: map[string]any{"body": map[string]any{"code": "SPEND_EVENLY"}}}})
		require.Nil(t, protocolErr)
		body, err := json.Marshal(result.StructuredContent)
		require.NoError(t, err)
		require.JSONEq(t, `{"data":{"code":"SPEND_EVENLY"}}`, string(body))
		httpResponse := httptest.NewRecorder()
		gateway.NewHandler(rt, nil, "test").ServeHTTP(httpResponse, httptest.NewRequest("POST", "/scoped-enum", strings.NewReader(`{"data":{"code":1}}`)))
		require.Equal(t, 200, httpResponse.Code, httpResponse.Body.String())
		require.JSONEq(t, `{"data":{"code":1}}`, httpResponse.Body.String())
	}
	require.Equal(t, 4, calls, "discovery must not execute the business handler")
}
