package runtime_test

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"net/http/httptest"
	"reflect"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/viant/bindly"
	"github.com/viant/datly/bootstrap"
	dexec "github.com/viant/datly/exec"
	httpgateway "github.com/viant/datly/gateway/http"
	nativeMCP "github.com/viant/datly/mcp"
	mcpserver "github.com/viant/datly/mcp/server"
	"github.com/viant/datly/output"
	"github.com/viant/datly/runtime"
	"github.com/viant/datly/runtime/handler/custom"
	"github.com/viant/datly/runtime/registry"
	"github.com/viant/datly/spec"
	"github.com/viant/mcp-protocol/authorization"
	oauthmeta "github.com/viant/mcp-protocol/oauth2/meta"
	"github.com/viant/mcp-protocol/schema"
	"github.com/viant/xdatly/contract"
	"github.com/viant/xdatly/response"
	"github.com/xeipuuv/gojsonschema"
)

type alternativeInput struct {
	Auth *presentationAuthOutput `parameter:",kind=component,in=GET:/alternative-auth,required=true"`
	Body *presentationRequest    `parameter:"Body,kind=body,in=" anonymous:"true"`
}

type formatCollisionRow struct {
	URLValue string
	UrlValue string
}
type formatCollisionOutput struct {
	Rows []formatCollisionRow `json:"rows"`
}

func (*formatCollisionOutput) Contract(context.Context, bindly.Binder) (*contract.Policy, error) {
	return &contract.Policy{Name: "format-collision/selected", DiscoveryAlternatives: []*contract.Policy{{Name: "format-collision/branch"}}}, nil
}

func TestNativeOutputDiscoveryIsolatesFormatFailures(t *testing.T) {
	artifact, err := bootstrap.BuildArtifact(bootstrap.ArtifactInput{Component: &spec.Component{Key: spec.Key{Kind: spec.KindComponent, Name: "FormatCollision"}, Routes: []*spec.Route{{Method: "GET", Path: "/format-collision", MCP: []*spec.MCPExposure{{Kind: spec.MCPExposureTool, Name: "CollisionUpper", CaseFormat: "uu"}, {Kind: spec.MCPExposureTool, Name: "CollisionLower", CaseFormat: "l"}}}}}, InputType: reflect.TypeFor[struct{}](), OutputType: reflect.TypeFor[formatCollisionOutput]()})
	require.NoError(t, err)
	calls := 0
	entry, err := artifact.Registration(registry.RegisteredComponent{Handler: custom.NewFunc(func(context.Context, *struct{}) (*formatCollisionOutput, error) {
		calls++
		return &formatCollisionOutput{}, nil
	})})
	require.NoError(t, err)
	rt, err := runtime.NewRuntime([]*registry.RegisteredComponent{entry})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, rt.Shutdown(context.Background())) })
	service, err := nativeMCP.New(nativeMCP.Config{Components: []*registry.RegisteredComponent{entry}, Invoker: rt})
	require.NoError(t, err)
	upper, ok := service.Registry().ToolRegistry.Get("CollisionUpper")
	require.True(t, ok)
	lower, ok := service.Registry().ToolRegistry.Get("CollisionLower")
	require.True(t, ok)
	for range 2 {
		metadata, _, err := service.DescribeTool(t.Context(), upper.Metadata)
		require.NoError(t, err)
		document, err := json.Marshal(metadata.OutputSchema)
		require.NoError(t, err)
		require.Contains(t, string(document), `"anyOf"`)
		_, _, err = service.DescribeTool(t.Context(), lower.Metadata)
		require.ErrorIs(t, err, output.ErrSchemaUnavailable)
		require.ErrorContains(t, err, "collision")
	}
	target := dexec.ComponentTarget{Component: entry.Component.Key, Route: spec.RouteRef{Method: "GET", Path: "/format-collision"}}
	first, err := rt.ResolveDiscoveryContract(t.Context(), target, nil)
	require.NoError(t, err)
	again, err := rt.ResolveDiscoveryContract(t.Context(), target, nil)
	require.NoError(t, err)
	require.Same(t, first.OutputAlternativeSchemas[0], again.OutputAlternativeSchemas[0], "warm discovery retains one native artifact with independent format outcomes")
	require.NotNil(t, first.OutputAlternativeSchemas[0].Schemas["uu"])
	require.ErrorIs(t, first.OutputAlternativeSchemas[0].Errors["l"], output.ErrSchemaUnavailable)
	require.Zero(t, calls)
}

type alternativeRecord struct {
	ImplicitName string
	Token        int     `json:"token" mcp:"name=publicToken" choice:"1:ONE" enumStyle:"literal"`
	Hidden       string  `json:"hidden" mcp:"-"`
	Secret       string  `json:"secret"`
	Name         *string `json:"name"`
	Empty        string  `json:"empty,omitempty"`
}
type alternativeExport struct {
	Exported bool `json:"exported"`
}
type alternativeRows struct {
	Summary []alternativeRecord `json:"summary"`
	Export  []alternativeExport `json:"export"`
}
type alternativeEnvelope struct {
	Rows  *alternativeRows `json:"rows"`
	Codes map[int]string   `json:"codes"`
}

var alternativePolicyCalls atomic.Int64

func alternativeState(ctx context.Context, binder bindly.Binder) (alternativeInput, error) {
	var state alternativeInput
	err := binder.Bind(ctx, &state)
	return state, err
}
func (*alternativeEnvelope) Contract(ctx context.Context, binder bindly.Binder) (*contract.Policy, error) {
	state, err := alternativeState(ctx, binder)
	if err != nil {
		return nil, err
	}
	return &contract.Policy{Name: "parent/" + state.Auth.Role, Field: func(_ string, parent reflect.Type, f *contract.Field) (*contract.Field, error) {
		if parent == reflect.TypeFor[alternativeRecord]() && f.Name == "Secret" && !strings.Contains(state.Auth.Role, "admin") {
			return f.Hide(), nil
		}
		return nil, nil
	}}, nil
}
func (*alternativeRows) Contract(ctx context.Context, binder bindly.Binder) (*contract.Policy, error) {
	state, err := alternativeState(ctx, binder)
	if err != nil {
		return nil, err
	}
	alternativePolicyCalls.Add(1)
	branch := func(holder string) *contract.Policy {
		return &contract.Policy{Name: "rows/" + holder, Field: func(_ string, parent reflect.Type, f *contract.Field) (*contract.Field, error) {
			if parent == reflect.TypeFor[alternativeRows]() {
				if f.Name != holder {
					return f.Hide(), nil
				}
				return f.SetTag("json", "data"), nil
			}
			return nil, nil
		}}
	}
	mode := "Summary"
	if state.Body != nil && state.Body.Mode == "export" {
		mode = "Export"
	}
	selected := branch(mode)
	selected.DiscoveryAlternatives = []*contract.Policy{branch("Summary"), branch("Export")}
	return selected, nil
}

func TestNativeOutputDiscoveryMetadataAuthorizationCallsAndREST(t *testing.T) {
	alternativePolicyCalls.Store(0)
	var authCalls, businessCalls atomic.Int64
	var authUnavailable atomic.Bool
	cause := errors.New("fixture auth unavailable")
	auth, err := bootstrap.BuildArtifact(bootstrap.ArtifactInput{Component: &spec.Component{Key: spec.Key{Kind: spec.KindComponent, Name: "AlternativeAuth"}, Routes: []*spec.Route{{Method: "GET", Path: "/alternative-auth"}}}, InputType: reflect.TypeFor[presentationAuthInput](), OutputType: reflect.TypeFor[presentationAuthOutput](), Handler: custom.NewFunc(func(_ context.Context, in *presentationAuthInput) (*presentationAuthOutput, error) {
		authCalls.Add(1)
		if authUnavailable.Load() || strings.Contains(in.Role, "unavailable") {
			return nil, &response.Error{Code: 503, Payload: map[string]any{"message": "auth unavailable", "status": 503}, Cause: cause}
		}
		return &presentationAuthOutput{Role: in.Role}, nil
	})})
	require.NoError(t, err)
	business, err := bootstrap.BuildArtifact(bootstrap.ArtifactInput{Component: &spec.Component{Key: spec.Key{Kind: spec.KindComponent, Name: "AlternativeBusiness"}, Routes: []*spec.Route{{Method: "POST", Path: "/alternative-business", MCP: []*spec.MCPExposure{{Kind: spec.MCPExposureTool, Name: "Alternatives", CaseFormat: "lc"}, {Kind: spec.MCPExposureTool, Name: "AlternativeSnake", CaseFormat: "lu"}}}}}, InputType: reflect.TypeFor[alternativeInput](), OutputType: reflect.TypeFor[alternativeEnvelope](), Handler: custom.NewFunc(func(context.Context, *alternativeInput) (*alternativeEnvelope, error) {
		businessCalls.Add(1)
		return &alternativeEnvelope{Rows: &alternativeRows{Summary: []alternativeRecord{{Token: 1, Hidden: "private", Secret: "allowed"}}, Export: []alternativeExport{{Exported: true}}}, Codes: map[int]string{7: "seven"}}, nil
	})})
	require.NoError(t, err)
	a, err := auth.Registration(registry.RegisteredComponent{})
	require.NoError(t, err)
	b, err := business.Registration(registry.RegisteredComponent{})
	require.NoError(t, err)
	entries := []*registry.RegisteredComponent{a, b}
	rt, err := runtime.NewRuntime(entries)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, rt.Shutdown(context.Background())) })
	service, err := nativeMCP.New(nativeMCP.Config{Components: entries, Invoker: rt, Authorization: &authorization.Policy{Tools: map[string]*authorization.Authorization{"Alternatives": {ProtectedResourceMetadata: &oauthmeta.ProtectedResourceMetadata{Resource: "http://alternative.test/mcp"}}, "AlternativeSnake": {ProtectedResourceMetadata: &oauthmeta.ProtectedResourceMetadata{Resource: "http://alternative.test/mcp"}}}}})
	require.NoError(t, err)
	server, err := mcpserver.New(mcpserver.Config{Service: service, Transport: mcpserver.TransportConfig{Kind: mcpserver.TransportStreamable}})
	require.NoError(t, err)
	httpServer, err := server.HTTP()
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, server.Shutdown(context.Background())) })
	for _, role := range []string{"reader", "admin", "reader", "unavailable"} {
		payload, err := json.Marshal(map[string]any{"jsonrpc": "2.0", "id": 1, "method": schema.MethodToolsList, "params": map[string]any{"_meta": map[string]any{"io.modelcontextprotocol/protocolVersion": schema.LatestProtocolVersion, "io.modelcontextprotocol/clientCapabilities": map[string]any{}}}})
		require.NoError(t, err)
		req := httptest.NewRequest("POST", "/mcp", bytes.NewReader(payload))
		req.Header.Set("Content-Type", "application/json")
		req.Header.Set("Accept", "application/json, text/event-stream")
		req.Header.Set(schema.HeaderProtocolVersion, schema.LatestProtocolVersion)
		req.Header.Set(schema.HeaderMethod, schema.MethodToolsList)
		req.Header.Set("Authorization", "Bearer "+role)
		rec := httptest.NewRecorder()
		beforeAuth := authCalls.Load()
		httpServer.Handler.ServeHTTP(rec, req)
		wantAuth := 2
		if role == "unavailable" {
			wantAuth = 1
		}
		require.EqualValues(t, wantAuth, authCalls.Load()-beforeAuth, "each published tool resolves authorization once; dependency failure stops publication")
		if role == "unavailable" {
			require.Contains(t, rec.Body.String(), `"error"`)
			require.NotContains(t, rec.Body.String(), `"tools":[]`)
		} else {
			require.Contains(t, rec.Body.String(), `"anyOf"`)
			if role == "admin" {
				require.Contains(t, rec.Body.String(), `"secret"`)
			} else {
				require.NotContains(t, rec.Body.String(), `"secret"`)
			}
		}
		require.Zero(t, businessCalls.Load())
	}
	tool, ok := service.Registry().ToolRegistry.Get("Alternatives")
	require.True(t, ok)
	for _, role := range []string{"reader", "reader", "admin", "reader"} {
		ctx := context.WithValue(t.Context(), authorization.TokenKey, &authorization.Token{Token: role})
		beforeAuth, beforePolicies := authCalls.Load(), alternativePolicyCalls.Load()
		metadata, _, err := service.DescribeTool(ctx, tool.Metadata)
		require.NoError(t, err)
		require.EqualValues(t, 1, authCalls.Load()-beforeAuth, "one auth dependency for parent, child and every branch")
		require.EqualValues(t, 1, alternativePolicyCalls.Load()-beforePolicies, "one output hook per discovery")
		require.Zero(t, businessCalls.Load())
		document, err := json.Marshal(metadata.OutputSchema)
		require.NoError(t, err)
		require.Contains(t, string(document), `"publicToken"`)
		require.Contains(t, string(document), `"implicitName"`)
		snake, ok := service.Registry().ToolRegistry.Get("AlternativeSnake")
		require.True(t, ok)
		snakeMetadata, _, err := service.DescribeTool(ctx, snake.Metadata)
		require.NoError(t, err)
		snakeDocument, err := json.Marshal(snakeMetadata.OutputSchema)
		require.NoError(t, err)
		require.Contains(t, string(snakeDocument), `"implicit_name"`)
		require.NotContains(t, string(snakeDocument), `"implicitName"`)
		require.Contains(t, string(document), `"ONE"`)
		require.NotContains(t, string(document), `"hidden"`)
		require.Contains(t, string(document), `"exported"`)
		require.Contains(t, string(document), `"additionalProperties":{"type":"string"}`)
		if role == "admin" {
			require.Contains(t, string(document), `"secret"`)
		} else {
			require.NotContains(t, string(document), `"secret"`)
		}
		// Publication clones cannot mutate the retained native schema artifact.
		metadata.OutputSchema.Type = "mutated"
		again, _, err := service.DescribeTool(ctx, tool.Metadata)
		require.NoError(t, err)
		require.Equal(t, "object", again.OutputSchema.Type)
	}
	ctx := context.WithValue(t.Context(), authorization.TokenKey, &authorization.Token{Token: "reader"})
	authUnavailable.Store(true)
	_, _, err = service.DescribeTool(ctx, tool.Metadata)
	require.ErrorIs(t, err, cause)
	authUnavailable.Store(false)
	metadata, _, err := service.DescribeTool(ctx, tool.Metadata)
	require.NoError(t, err)
	document, err := json.Marshal(metadata.OutputSchema)
	require.NoError(t, err)
	for _, mode := range []string{"summary", "export", "summary"} {
		beforeAuth, beforePolicies := authCalls.Load(), alternativePolicyCalls.Load()
		result, rpc := tool.Handler(ctx, &schema.CallToolRequest{Method: schema.MethodToolsCall, Params: schema.CallToolRequestParams{Name: "Alternatives", Arguments: map[string]any{"mode": mode}}})
		require.Nil(t, rpc)
		require.False(t, result.IsError != nil && *result.IsError)
		encoded, err := json.Marshal(result.StructuredContent)
		require.NoError(t, err)
		if mode == "export" {
			require.JSONEq(t, `{"rows":{"data":[{"exported":true}]},"codes":{"7":"seven"}}`, string(encoded))
		} else {
			require.JSONEq(t, `{"rows":{"data":[{"implicitName":"","publicToken":"ONE","name":null}]},"codes":{"7":"seven"}}`, string(encoded))
		}
		validation, err := gojsonschema.Validate(gojsonschema.NewBytesLoader(document), gojsonschema.NewBytesLoader(encoded))
		require.NoError(t, err)
		require.True(t, validation.Valid(), "%v", validation.Errors())
		require.EqualValues(t, 1, authCalls.Load()-beforeAuth, "call authorization is scoped to the operation")
		require.EqualValues(t, 2, alternativePolicyCalls.Load()-beforePolicies, "presentation re-resolution uses actual call arguments")
	}
	before := businessCalls.Load()
	unavailable := context.WithValue(t.Context(), authorization.TokenKey, &authorization.Token{Token: "unavailable"})
	_, _, err = service.DescribeTool(unavailable, tool.Metadata)
	require.ErrorIs(t, err, cause)
	require.Equal(t, before, businessCalls.Load())
	failed, rpc := tool.Handler(unavailable, &schema.CallToolRequest{Method: schema.MethodToolsCall, Params: schema.CallToolRequestParams{Name: "Alternatives", Arguments: map[string]any{}}})
	require.Nil(t, rpc)
	require.True(t, *failed.IsError)
	failure, err := json.Marshal(failed)
	require.NoError(t, err)
	require.Contains(t, string(failure), `"io.viant/http-status":503`)
	require.Equal(t, before, businessCalls.Load())
	target := dexec.ComponentTarget{Component: b.Component.Key, Route: spec.RouteRef{Method: "POST", Path: "/alternative-business"}}
	cancelled, cancel := context.WithCancel(t.Context())
	cancel()
	_, err = rt.ResolveDiscoveryContract(cancelled, target, nil)
	require.ErrorIs(t, err, context.Canceled)
	// HTTP still serializes only its selected policy with ordinary JSON names.
	request := httptest.NewRequest("POST", "/alternative-business", strings.NewReader(`{"mode":"summary"}`))
	request.Header.Set("Content-Type", "application/json")
	request.Header.Set("Authorization", "reader")
	rec := httptest.NewRecorder()
	httpgateway.NewHandler(rt, nil, "output-discovery").ServeHTTP(rec, request)
	require.Equal(t, 200, rec.Code, rec.Body.String())
	require.JSONEq(t, `{"rows":{"data":[{"ImplicitName":"","token":"ONE","hidden":"private","name":null}]},"codes":{"7":"seven"}}`, rec.Body.String())
}
