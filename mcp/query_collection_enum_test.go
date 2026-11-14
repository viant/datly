package mcp_test

import (
	"context"
	"net/http/httptest"
	"reflect"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/viant/bindly"
	"github.com/viant/datly/bootstrap"
	gateway "github.com/viant/datly/gateway/http"
	"github.com/viant/datly/internal/testharness/mcpclient"
	"github.com/viant/datly/mcp"
	druntime "github.com/viant/datly/runtime"
	"github.com/viant/datly/runtime/handler/custom"
	"github.com/viant/datly/runtime/registry"
	"github.com/viant/datly/spec"
	"github.com/viant/mcp-protocol/schema"
	xcontract "github.com/viant/xdatly/contract"
	xmcp "github.com/viant/xdatly/handler/mcp"
)

type queryCollectionEnumInput struct {
	Codes    []int    `parameter:",kind=query,in=codes" mcpEnum:"true" choice:"7:ALPHA" choice:"3:BETA" enumAccept:"literal" enumStyle:"literal"`
	Ordinary []int    `parameter:",kind=query,in=ordinary"`
	Labels   []string `parameter:",kind=query,in=labels" mcpEnum:"true" choice:"ALPHA" choice:"BETA"`
}

type queryCollectionEnumOutput struct {
	OK bool `json:"ok"`
}

func (*queryCollectionEnumInput) Contract(ctx context.Context, _ bindly.Binder) (*xcontract.Policy, error) {
	if _, ok := xmcp.LookupContext(ctx); ok {
		return nil, nil
	}
	return &xcontract.Policy{Name: "query-collection-rest-storage:v1", Field: func(_ string, _ reflect.Type, field *xcontract.Field) (*xcontract.Field, error) {
		if field.Tag.Get("mcpEnum") != "true" {
			return nil, nil
		}
		return field.RemoveTag("choice").RemoveTag("enumAccept").RemoveTag("enumStyle"), nil
	}}, nil
}

func TestMCPQueryCollectionEnumNativeTransportAndREST(t *testing.T) {
	component := &spec.Component{Key: spec.Key{Kind: spec.KindComponent, Name: "query_collection_enum"}, Routes: []*spec.Route{{Method: "GET", Path: "/query-collection-enum", MCP: []*spec.MCPExposure{{Kind: spec.MCPExposureTool, Name: "query_collection_enum"}}}}}
	artifact, err := bootstrap.BuildArtifact(bootstrap.ArtifactInput{Component: component, InputType: reflect.TypeFor[queryCollectionEnumInput](), OutputType: reflect.TypeFor[queryCollectionEnumOutput]()})
	require.NoError(t, err)
	var captured *queryCollectionEnumInput
	calls := 0
	entry, err := artifact.Registration(registry.RegisteredComponent{Handler: custom.NewFunc(func(_ context.Context, in *queryCollectionEnumInput) (*queryCollectionEnumOutput, error) {
		calls++
		captured = in
		return &queryCollectionEnumOutput{OK: true}, nil
	})})
	require.NoError(t, err)
	runtime, err := druntime.NewRuntime([]*registry.RegisteredComponent{entry})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, runtime.Shutdown(context.Background())) })
	service, err := mcp.New(mcp.Config{Components: []*registry.RegisteredComponent{entry}, Invoker: runtime})
	require.NoError(t, err)
	native := mcpclient.New(t, service, schema.LatestProtocolVersion)
	listed, err := native.ListTools(t.Context(), nil)
	require.NoError(t, err)
	require.Len(t, listed.Tools, 1)
	property := listed.Tools[0].InputSchema.Properties["codes"]
	require.Equal(t, []any{"ALPHA", "BETA"}, property["items"].(map[string]any)["enum"])
	for _, tc := range []struct {
		name  string
		args  map[string]any
		valid bool
		codes []int
	}{
		{"mapped", map[string]any{"codes": []any{"BETA", "ALPHA", "BETA"}, "ordinary": []any{19, 23}, "labels": []any{"BETA"}}, true, []int{3, 7, 3}},
		{"empty", map[string]any{"codes": []any{}}, true, []int{}},
		{"null", map[string]any{"codes": nil}, true, nil},
		{"omitted", map[string]any{}, true, nil},
		{"unknown", map[string]any{"codes": []any{"UNKNOWN"}}, false, nil},
		{"numeric representation", map[string]any{"codes": []any{7}}, false, nil},
		{"null item retains scalar owner", map[string]any{"codes": []any{nil}}, true, []int{0}},
		{"scalar", map[string]any{"codes": "ALPHA"}, false, nil},
		{"ordinary strings", map[string]any{"ordinary": []any{"ALPHA"}}, false, nil},
	} {
		t.Run(tc.name, func(t *testing.T) {
			before := calls
			result, err := native.CallTool(t.Context(), &schema.CallToolRequestParams{Name: "query_collection_enum", Arguments: tc.args})
			if !tc.valid {
				require.Error(t, err)
				require.Contains(t, err.Error(), "-32602")
				require.Equal(t, before, calls)
				return
			}
			require.NoError(t, err)
			require.NotNil(t, result)
			require.False(t, result.IsError != nil && *result.IsError, "%+v", result)
			require.Equal(t, before+1, calls)
			require.Equal(t, tc.codes, captured.Codes)
			if tc.name == "mapped" {
				require.Equal(t, []int{19, 23}, captured.Ordinary)
				require.Equal(t, []string{"BETA"}, captured.Labels)
			}
		})
	}
	response := httptest.NewRecorder()
	gateway.NewHandler(runtime, nil, "test").ServeHTTP(response, httptest.NewRequest("GET", "/query-collection-enum?codes=19&codes=23&ordinary=31&labels=UNKNOWN", nil))
	require.Equal(t, 200, response.Code, response.Body.String())
	require.Equal(t, []int{19, 23}, captured.Codes)
	require.Equal(t, []string{"UNKNOWN"}, captured.Labels)
}
