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
	"github.com/viant/datly/exec"
	gateway "github.com/viant/datly/gateway/http"
	"github.com/viant/datly/internal/testharness/mcpclient"
	"github.com/viant/datly/mcp"
	druntime "github.com/viant/datly/runtime"
	"github.com/viant/datly/runtime/handler/custom"
	"github.com/viant/datly/spec"
	"github.com/viant/mcp-protocol/schema"
	"github.com/viant/tagly/choice"
	xcontract "github.com/viant/xdatly/contract"
	xmcp "github.com/viant/xdatly/handler/mcp"
	"github.com/xeipuuv/gojsonschema"
)

type booleanEnumRecord struct {
	Flag int                  `json:"flag" choice:"0:false" choice:"1:true" enumAccept:"boolean" enumStyle:"boolean" enumNull:"reject"`
	Has  *struct{ Flag bool } `json:"-" setMarker:"true"`
}

// The application owns transport scoping; this fixture exercises the same native
// contract mechanism without changing the stored integer or REST representation.
func (*booleanEnumRecord) Contract(ctx context.Context, _ bindly.Binder) (*xcontract.Policy, error) {
	if _, ok := xmcp.LookupContext(ctx); ok {
		return nil, nil
	}
	return &xcontract.Policy{Name: "boolean-enum-http-storage:v1", Field: func(_ string, parent reflect.Type, field *xcontract.Field) (*xcontract.Field, error) {
		if parent == reflect.TypeFor[booleanEnumRecord]() && field.Name == "Flag" {
			return field.RemoveTag("choice").RemoveTag("enumAccept").RemoveTag("enumStyle").RemoveTag("enumNull"), nil
		}
		return nil, nil
	}}, nil
}

type booleanEnumInput struct {
	Body *booleanEnumRecord `parameter:",kind=body,in=data"`
}
type booleanEnumOutput struct {
	Data *booleanEnumRecord `json:"data"`
}
type booleanEnumRecorder struct {
	*druntime.Runtime
	err error
}

func (r *booleanEnumRecorder) InvokeComponent(ctx context.Context, req exec.ComponentRequest) (any, error) {
	result, err := r.Runtime.InvokeComponent(ctx, req)
	r.err = err
	return result, err
}

func TestBooleanEnumNativeMCPHTTPAndREST(t *testing.T) {
	strict := true
	component := &spec.Component{Key: spec.Key{Kind: spec.KindComponent, Name: "boolean_enum"}, Routes: []*spec.Route{{Method: "POST", Path: "/boolean-enum", MCP: []*spec.MCPExposure{{Kind: spec.MCPExposureTool, Name: "boolean_enum", CaseFormat: "lc", StrictArguments: &strict}}}}}
	artifact, err := bootstrap.BuildArtifact(bootstrap.ArtifactInput{Component: component, InputType: reflect.TypeFor[booleanEnumInput](), OutputType: reflect.TypeFor[booleanEnumOutput]()})
	require.NoError(t, err)
	calls := 0
	var bound *booleanEnumRecord
	registered, err := artifact.Registration(druntime.RegisteredComponent{Handler: custom.NewFunc(func(_ context.Context, in *booleanEnumInput) (*booleanEnumOutput, error) {
		calls++
		bound = in.Body
		return &booleanEnumOutput{Data: in.Body}, nil
	})})
	require.NoError(t, err)
	rt, err := druntime.NewRuntime([]*druntime.RegisteredComponent{registered})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, rt.Shutdown(context.Background())) })
	// Embedding Runtime preserves its optional ResolveContract capability.
	recorder := &booleanEnumRecorder{Runtime: rt}
	service, err := mcp.New(mcp.Config{Components: []*druntime.RegisteredComponent{registered}, Invoker: recorder})
	require.NoError(t, err)
	native := mcpclient.New(t, service, schema.LatestProtocolVersion)
	for range 2 {
		before := calls
		listed, err := native.ListTools(t.Context(), nil)
		require.NoError(t, err)
		require.Len(t, listed.Tools, 1)
		require.Equal(t, before, calls)
		inputRaw, err := json.Marshal(listed.Tools[0].InputSchema)
		require.NoError(t, err)
		var inputSchema map[string]any
		require.NoError(t, json.Unmarshal(inputRaw, &inputSchema))
		flag := inputSchema["properties"].(map[string]any)["body"].(map[string]any)["properties"].(map[string]any)["flag"].(map[string]any)
		require.Equal(t, "boolean", flag["type"])
		validator, err := gojsonschema.NewSchema(gojsonschema.NewBytesLoader(inputRaw))
		require.NoError(t, err)
		for _, tc := range []struct {
			raw            string
			valid, present bool
			value          int
		}{
			{`{"body":{"flag":true}}`, true, true, 1},
			{`{"body":{"flag":false}}`, true, true, 0},
			{`{"body":{}}`, true, false, 0},
			{`{"body":{"flag":null}}`, false, false, 0},
			{`{"body":{"flag":0}}`, false, false, 0},
			{`{"body":{"flag":1}}`, false, false, 0},
			{`{"body":{"flag":0.5}}`, false, false, 0},
			{`{"body":{"flag":"true"}}`, false, false, 0},
			{`{"body":{"flag":"1"}}`, false, false, 0},
		} {
			validation, err := validator.Validate(gojsonschema.NewStringLoader(tc.raw))
			require.NoError(t, err)
			require.Equal(t, tc.valid, validation.Valid(), tc.raw)
			before := calls
			var arguments map[string]any
			require.NoError(t, json.Unmarshal([]byte(tc.raw), &arguments))
			reply, err := native.CallTool(t.Context(), &schema.CallToolRequestParams{Name: "boolean_enum", Arguments: arguments})
			require.NoError(t, err)
			wire, err := json.Marshal(reply)
			require.NoError(t, err)
			require.Equal(t, !tc.valid, reply.IsError != nil && *reply.IsError, string(wire))
			content, err := json.Marshal(reply.StructuredContent)
			require.NoError(t, err)
			if !tc.valid {
				require.Equal(t, before, calls)
				var failure struct {
					Status  int
					Message string
				}
				require.NoError(t, json.Unmarshal(content, &failure))
				require.Equal(t, 400, failure.Status, string(wire))
				require.NotEmpty(t, failure.Message)
				if strings.Contains(tc.raw, "null") {
					require.ErrorIs(t, recorder.err, choice.ErrNullInput)
					require.Equal(t, choice.ErrNullInput.Error(), failure.Message)
				}
				continue
			}
			require.Equal(t, before+1, calls)
			require.NotNil(t, bound)
			require.NotNil(t, bound.Has)
			require.Equal(t, tc.value, bound.Flag)
			require.Equal(t, tc.present, bound.Has.Flag)
			expected := `{"data":{"flag":false}}`
			if tc.value == 1 {
				expected = `{"data":{"flag":true}}`
			}
			require.JSONEq(t, expected, string(content))
		}
		for _, value := range []string{"0", "1"} {
			response := httptest.NewRecorder()
			before := calls
			gateway.NewHandler(rt, nil, "test").ServeHTTP(response, httptest.NewRequest("POST", "/boolean-enum", strings.NewReader(`{"data":{"flag":`+value+`}}`)))
			require.Equal(t, 200, response.Code, response.Body.String())
			require.Equal(t, before+1, calls)
			require.True(t, bound.Has.Flag)
			require.JSONEq(t, `{"data":{"flag":`+value+`}}`, response.Body.String())
		}
	}
}
