package mcp_test

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/viant/bindly"
	"github.com/viant/datly/mcp/invocation"
	xcontract "github.com/viant/xdatly/contract"
	"github.com/viant/xdatly/response"
	"net/http"
	"net/http/httptest"
	"reflect"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/viant/datly/bootstrap"
	gateway "github.com/viant/datly/gateway/http"
	"github.com/viant/datly/mcp"
	druntime "github.com/viant/datly/runtime"
	"github.com/viant/datly/runtime/handler/custom"
	"github.com/viant/datly/spec"
	"github.com/viant/mcp-protocol/schema"
)

type visibilityChild struct {
	Public string `json:"public"`
	Secret string `json:"secret" mcp:"-"`
}
type VisibilityEmbedded struct {
	EmbeddedSecret string `json:"embeddedSecret" mcp:"-"`
	EmbeddedPublic string `json:"embeddedPublic"`
}

type visibilityOutput struct {
	*VisibilityEmbedded
	Secret string                     `json:"secret" mcp:"-"`
	Rows   []*visibilityChild         `json:"rows"`
	Values map[string]visibilityChild `json:"values"`
}

func TestMCPOutputVisibilityPreservesHTTPContract(t *testing.T) {
	for _, format := range []string{"", "lc"} {
		t.Run("format="+format, func(t *testing.T) {
			component := &spec.Component{Key: spec.Key{Kind: spec.KindComponent, Name: "Visibility"}, Routes: []*spec.Route{{Method: "GET", Path: "/visibility", MCP: []*spec.MCPExposure{{Kind: spec.MCPExposureTool, Name: "visibility", CaseFormat: format}}}}}
			artifact, err := bootstrap.BuildArtifact(bootstrap.ArtifactInput{Component: component, InputType: reflect.TypeFor[struct{}](), OutputType: reflect.TypeFor[visibilityOutput]()})
			require.NoError(t, err)
			value := visibilityOutput{VisibilityEmbedded: &VisibilityEmbedded{EmbeddedSecret: "private", EmbeddedPublic: "embedded"}, Secret: "private", Rows: []*visibilityChild{{Public: "row", Secret: "private"}, nil}, Values: map[string]visibilityChild{"key": {Public: "map", Secret: "private"}}}
			registered, err := artifact.Registration(druntime.RegisteredComponent{Handler: custom.NewFunc[struct{}, visibilityOutput](func(context.Context, *struct{}) (*visibilityOutput, error) { return &value, nil })})
			require.NoError(t, err)
			rt, err := druntime.NewRuntime([]*druntime.RegisteredComponent{registered})
			require.NoError(t, err)
			t.Cleanup(func() { require.NoError(t, rt.Shutdown(context.Background())) })
			service, err := mcp.New(mcp.Config{Components: []*druntime.RegisteredComponent{registered}, Invoker: rt})
			require.NoError(t, err)
			entry, ok := service.Registry().ToolRegistry.Get("visibility")
			require.True(t, ok)
			metadata, err := json.Marshal(entry.Metadata.OutputSchema)
			require.NoError(t, err)
			require.NotContains(t, string(metadata), "secret")
			require.NotContains(t, string(metadata), "embeddedSecret")
			result, protocolErr := entry.Handler(t.Context(), &schema.CallToolRequest{Method: schema.MethodToolsCall, Params: schema.CallToolRequestParams{Name: "visibility"}})
			require.Nil(t, protocolErr)
			encoded, err := json.Marshal(result.StructuredContent)
			require.NoError(t, err)
			require.JSONEq(t, `{"embeddedPublic":"embedded","rows":[{"public":"row"},null],"values":{"key":{"public":"map"}}}`, string(encoded))
			response := httptest.NewRecorder()
			gateway.NewHandler(rt, nil, "test").ServeHTTP(response, httptest.NewRequest("GET", "/visibility", nil))
			require.Equal(t, 200, response.Code, response.Body.String())
			require.Contains(t, response.Body.String(), `"secret":"private"`)
			require.Contains(t, response.Body.String(), `"embeddedSecret":"private"`)
		})
	}
}

type dynamicVisibilityOutput struct {
	Secret  string `json:"secret" mcp:"-"`
	Dynamic string `json:"dynamic"`
}

func (*dynamicVisibilityOutput) Contract(ctx context.Context, binder bindly.Binder) (*xcontract.Policy, error) {
	var state struct {
		Show bool `parameter:",kind=header,in=X-Show"`
	}
	if err := binder.Bind(ctx, &state); err != nil {
		return nil, err
	}
	return &xcontract.Policy{Name: fmt.Sprintf("visibility:%t", state.Show), Field: func(_ string, parent reflect.Type, field *xcontract.Field) (*xcontract.Field, error) {
		if parent == reflect.TypeFor[dynamicVisibilityOutput]() && field.Name == "Dynamic" {
			field = field.SetTag("json", "renamed")
			if !state.Show {
				field = field.SetTag("mcp", "-")
			}
			return field, nil
		}
		return nil, nil
	}}, nil
}

func TestMCPOutputVisibilityUsesSelectedContractAndErrors(t *testing.T) {
	for _, fail := range []bool{false, true} {
		t.Run(fmt.Sprintf("error=%t", fail), func(t *testing.T) {
			component := &spec.Component{Key: spec.Key{Kind: spec.KindComponent, Name: "DynamicVisibility"}, Routes: []*spec.Route{{Method: "GET", Path: "/dynamic-visibility", MCP: []*spec.MCPExposure{{Kind: spec.MCPExposureTool, Name: "visibility"}}}}}
			artifact, err := bootstrap.BuildArtifact(bootstrap.ArtifactInput{Component: component, InputType: reflect.TypeFor[struct{}](), OutputType: reflect.TypeFor[dynamicVisibilityOutput]()})
			require.NoError(t, err)
			registered, err := artifact.Registration(druntime.RegisteredComponent{Handler: custom.NewFunc[struct{}, dynamicVisibilityOutput](func(context.Context, *struct{}) (*dynamicVisibilityOutput, error) {
				value := &dynamicVisibilityOutput{Secret: "private", Dynamic: "conditional"}
				if fail {
					return value, &response.Error{Code: 400, Payload: value}
				}
				return value, nil
			})})
			require.NoError(t, err)
			rt, err := druntime.NewRuntime([]*druntime.RegisteredComponent{registered})
			require.NoError(t, err)
			t.Cleanup(func() { require.NoError(t, rt.Shutdown(context.Background())) })
			service, err := mcp.New(mcp.Config{Components: []*druntime.RegisteredComponent{registered}, Invoker: rt})
			require.NoError(t, err)
			entry, ok := service.Registry().ToolRegistry.Get("visibility")
			require.True(t, ok)
			for _, show := range []bool{false, true, false} {
				ctx := invocation.WithHTTPHeaders(t.Context(), http.Header{"X-Show": {fmt.Sprint(show)}})
				metadata, _, err := service.DescribeTool(ctx, entry.Metadata)
				require.NoError(t, err)
				require.NotContains(t, metadata.OutputSchema.Properties, "secret")
				_, present := metadata.OutputSchema.Properties["renamed"]
				require.Equal(t, show, present)
				result, protocolErr := entry.Handler(ctx, &schema.CallToolRequest{Method: schema.MethodToolsCall, Params: schema.CallToolRequestParams{Name: "visibility"}})
				require.Nil(t, protocolErr)
				require.Equal(t, fail, result.IsError != nil && *result.IsError)
				encoded, err := json.Marshal(result.StructuredContent)
				require.NoError(t, err)
				expected := `{}`
				if show {
					expected = `{"renamed":"conditional"}`
				}
				require.JSONEq(t, expected, string(encoded))
			}
		})
	}
}
