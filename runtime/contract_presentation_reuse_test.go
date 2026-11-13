package runtime_test

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"reflect"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/viant/bindly"
	"github.com/viant/bindly/locator"
	requestprovider "github.com/viant/bindly/provider/request"
	"github.com/viant/datly/bootstrap"
	dexec "github.com/viant/datly/exec"
	httpgateway "github.com/viant/datly/gateway/http"
	"github.com/viant/datly/internal/testharness"
	nativeMCP "github.com/viant/datly/mcp"
	mcpserver "github.com/viant/datly/mcp/server"
	"github.com/viant/datly/runtime"
	runtimeauth "github.com/viant/datly/runtime/auth"
	"github.com/viant/datly/runtime/handler/custom"
	"github.com/viant/datly/runtime/handler/provider"
	"github.com/viant/datly/runtime/registry"
	"github.com/viant/datly/spec"
	"github.com/viant/mcp-protocol/authorization"
	oauthmeta "github.com/viant/mcp-protocol/oauth2/meta"
	"github.com/viant/mcp-protocol/schema"
	"github.com/viant/scy/auth/jwt"
	"github.com/viant/xdatly/contract"
	xresponse "github.com/viant/xdatly/response"
)

type presentationRequest struct {
	Mode   string   `json:"mode,omitempty"`
	Fields []string `json:"fields,omitempty"`
}
type presentationAuthInput struct {
	Role string `parameter:",kind=header,in=Authorization,required=true"`
}
type presentationVerifiedAuthInput struct {
	Claims *jwt.Claims `parameter:",kind=header,in=Authorization,dataType=string,required=true,errorCode=401" codec:"JwtClaim"`
}
type presentationAuthOutput struct {
	Role     string
	Policies *atomic.Int64
}
type presentationInput struct {
	Auth   *presentationAuthOutput `parameter:",kind=component,in=GET:/presentation-auth,required=true"`
	Body   *presentationRequest    `parameter:"Body,kind=body,in=" anonymous:"true"`
	Query  string                  `parameter:",kind=query,in=variant"`
	Header string                  `parameter:",kind=header,in=X-Variant"`
}
type presentationSummary struct {
	ID       int  `json:"id"`
	Writable bool `json:"writable"`
}
type presentationEntry struct {
	SiteID int     `json:"siteId"`
	Name   *string `json:"name"`
}
type presentationOutput struct {
	Role      string                `json:"role"`
	Summaries []presentationSummary `json:"summaries"`
	Entries   []presentationEntry   `json:"entries"`
}

func (*presentationOutput) Contract(ctx context.Context, binder bindly.Binder) (*contract.Policy, error) {
	var state presentationInput
	if err := binder.Bind(ctx, &state); err != nil {
		return nil, err
	}
	state.Auth.Policies.Add(1)
	mode := "summary"
	if state.Header != "" {
		mode = state.Header
	}
	if state.Query != "" {
		mode = state.Query
	}
	var fields []string
	if state.Body != nil {
		if state.Body.Mode != "" {
			mode = state.Body.Mode
		}
		fields = state.Body.Fields
	}
	for _, f := range fields {
		if f != "id" && f != "writable" {
			return nil, &xresponse.Error{Code: 400, Payload: map[string]any{"message": "unknown presentation field", "status": 400}, Cause: fmt.Errorf("unknown presentation field")}
		}
	}
	return &contract.Policy{Name: state.Auth.Role + ":" + mode + ":" + strings.Join(fields, ","), Field: func(_ string, parent reflect.Type, f *contract.Field) (*contract.Field, error) {
		if parent == reflect.TypeFor[presentationOutput]() {
			if f.Name == "Summaries" && mode != "summary" || f.Name == "Entries" && mode != "entries" {
				return f.Hide(), nil
			}
			if f.Name == "Summaries" || f.Name == "Entries" {
				return f.SetTag("json", "data"), nil
			}
		}
		if parent == reflect.TypeFor[presentationSummary]() && len(fields) > 0 {
			name := strings.Split(f.Tag.Get("json"), ",")[0]
			for _, selected := range fields {
				if name == selected {
					return nil, nil
				}
			}
			return f.Hide(), nil
		}
		return nil, nil
	}}, nil
}
func newPresentationFixture(t testing.TB, keys ...*testharness.JWT) (*runtime.Runtime, []*registry.RegisteredComponent, *atomic.Int64, *atomic.Int64) {
	t.Helper()
	calls, policies := new(atomic.Int64), new(atomic.Int64)
	authInput := bootstrap.ArtifactInput{Component: &spec.Component{Key: spec.Key{Kind: spec.KindComponent, Name: "PresentationAuth"}, Routes: []*spec.Route{{Method: "GET", Path: "/presentation-auth"}}}, InputType: reflect.TypeFor[presentationAuthInput](), OutputType: reflect.TypeFor[presentationAuthOutput](), Handler: custom.NewFunc(func(_ context.Context, in *presentationAuthInput) (*presentationAuthOutput, error) {
		calls.Add(1)
		return &presentationAuthOutput{Role: in.Role, Policies: policies}, nil
	})}
	if len(keys) > 0 {
		codecs, err := runtimeauth.New(t.Context(), &runtimeauth.Config{JWTValidator: keys[0].Config()})
		require.NoError(t, err)
		authInput.InputType = reflect.TypeFor[presentationVerifiedAuthInput]()
		authInput.CodecFactory = codecs
		authInput.Handler = custom.NewFunc(func(_ context.Context, in *presentationVerifiedAuthInput) (*presentationAuthOutput, error) {
			calls.Add(1)
			return &presentationAuthOutput{Role: in.Claims.Subject, Policies: policies}, nil
		})
	}
	auth, err := bootstrap.BuildArtifact(authInput)
	require.NoError(t, err)
	business, err := bootstrap.BuildArtifact(bootstrap.ArtifactInput{Component: &spec.Component{Key: spec.Key{Kind: spec.KindComponent, Name: "Presentation"}, Routes: []*spec.Route{{Method: "POST", Path: "/presentation", MCP: []*spec.MCPExposure{{Kind: spec.MCPExposureTool, Name: "Presentation"}}}}}, InputType: reflect.TypeFor[presentationInput](), OutputType: reflect.TypeFor[presentationOutput](), Handler: custom.NewFunc(func(_ context.Context, in *presentationInput) (*presentationOutput, error) {
		return &presentationOutput{Role: in.Auth.Role, Summaries: []presentationSummary{{ID: 7, Writable: true}}, Entries: []presentationEntry{{SiteID: 9}}}, nil
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
	return rt, entries, calls, policies
}
func presentationScope(t *testing.T, body, query, header, role string) *requestprovider.Scope {
	t.Helper()
	req := httptest.NewRequest("POST", "/presentation"+query, strings.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Authorization", role)
	if header != "" {
		req.Header.Set("X-Variant", header)
	}
	scope, err := requestprovider.New(req)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, scope.Close()) })
	return scope
}
func presentationTarget() dexec.ComponentTarget {
	return dexec.ComponentTarget{Component: spec.Key{Kind: spec.KindComponent, Name: "Presentation"}, Route: spec.RouteRef{Method: "POST", Path: "/presentation"}}
}
func TestContractPresentationHandoffTracksProtocolSources(t *testing.T) {
	for _, tc := range []struct {
		name, before, after, bq, aq, bh, ah string
		changed                             bool
	}{
		{name: "same absent"},
		{name: "body absent to present", after: `{"mode":"entries"}`, changed: true},
		{name: "body present to changed", before: `{"mode":"entries"}`, after: `{"mode":"summary"}`, changed: true},
		{name: "body present to absent", before: `{"mode":"entries"}`, changed: true},
		{name: "query absent to present", aq: "?variant=entries", changed: true},
		{name: "query present to absent", bq: "?variant=entries", changed: true},
		{name: "query changed", bq: "?variant=entries", aq: "?variant=summary", changed: true},
		{name: "same query", bq: "?variant=entries", aq: "?variant=entries"},
		{name: "header absent to present", ah: "entries", changed: true},
		{name: "header present to absent", bh: "entries", changed: true},
		{name: "same header", bh: "entries", ah: "entries"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			rt, _, calls, policies := newPresentationFixture(t)
			ctx := dexec.CaptureContracts(t.Context())
			target := presentationTarget()
			initial, err := rt.ResolveContract(ctx, target, presentationScope(t, tc.before, tc.bq, tc.bh, "first"))
			require.NoError(t, err)
			dexec.PublishContract(ctx, target, initial)
			scope := presentationScope(t, tc.after, tc.aq, tc.ah, "first")
			_, err = rt.InvokeComponent(ctx, dexec.ComponentRequest{Target: target, Providers: scope.Providers()})
			require.NoError(t, err)
			require.EqualValues(t, 1, calls.Load(), "matching same-operation dependency must survive presentation re-resolution")
			want := int64(1)
			if tc.changed {
				want++
			}
			require.Equal(t, want, policies.Load())
			effective, ok := dexec.SelectedContract(ctx, target)
			require.True(t, ok)
			expectEntries := strings.Contains(tc.after, "entries") || tc.after == "" && (strings.Contains(tc.aq, "entries") || tc.aq == "" && tc.ah == "entries")
			f, _ := reflect.TypeFor[presentationOutput]().FieldByName("Entries")
			if expectEntries {
				require.Equal(t, "data", effective.OutputRepresentation.Field(f.Index[0]).Tag.Get("json"))
			} else {
				require.Equal(t, "-", effective.OutputRepresentation.Field(f.Index[0]).Tag.Get("json"))
			}
			// A completed operation's selected receipt cannot authorize another operation.
			_, err = rt.InvokeComponent(ctx, dexec.ComponentRequest{Target: target, Providers: scope.Providers()})
			require.NoError(t, err)
			require.EqualValues(t, 2, calls.Load())
		})
	}
}
func TestContractPresentationHandoffChangedCredentialsReauthorizes(t *testing.T) {
	rt, _, calls, _ := newPresentationFixture(t)
	ctx := dexec.CaptureContracts(t.Context())
	target := presentationTarget()
	initial, err := rt.ResolveContract(ctx, target, presentationScope(t, "", "", "", "first"))
	require.NoError(t, err)
	dexec.PublishContract(ctx, target, initial)
	scope := presentationScope(t, `{"mode":"entries"}`, "", "", "second")
	out, err := rt.InvokeComponent(ctx, dexec.ComponentRequest{Target: target, Providers: scope.Providers()})
	require.NoError(t, err)
	require.Equal(t, "second", out.(*presentationOutput).Role)
	require.EqualValues(t, 2, calls.Load())
}

func TestStreamableContractPresentationUsesActualArguments(t *testing.T) {
	key := testharness.NewJWT(t)
	rt, entries, calls, _ := newPresentationFixture(t, key)
	service, err := nativeMCP.New(nativeMCP.Config{Components: entries, Invoker: rt, Authorization: presentationAuthorization()})
	require.NoError(t, err)
	server, err := mcpserver.New(mcpserver.Config{Service: service, Transport: mcpserver.TransportConfig{Kind: mcpserver.TransportStreamable}})
	require.NoError(t, err)
	httpServer, err := server.HTTP()
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, server.Shutdown(context.Background())) })
	rpc := func(t *testing.T, method string, args map[string]any, role string) json.RawMessage {
		t.Helper()
		params := map[string]any{"_meta": map[string]any{"io.modelcontextprotocol/protocolVersion": schema.LatestProtocolVersion, "io.modelcontextprotocol/clientCapabilities": map[string]any{}}}
		if method == schema.MethodToolsCall {
			params["name"] = "Presentation"
			params["arguments"] = args
		}
		data, err := json.Marshal(map[string]any{"jsonrpc": "2.0", "id": 1, "method": method, "params": params})
		require.NoError(t, err)
		req := httptest.NewRequest("POST", "/mcp", bytes.NewReader(data))
		req.Header.Set("Content-Type", "application/json")
		req.Header.Set("Accept", "application/json, text/event-stream")
		req.Header.Set(schema.HeaderProtocolVersion, schema.LatestProtocolVersion)
		req.Header.Set(schema.HeaderMethod, method)
		req.Header.Set("Authorization", key.Sign(t, role, time.Now().Add(time.Hour)))
		if method == schema.MethodToolsCall {
			req.Header.Set(schema.HeaderName, "Presentation")
		}
		rec := httptest.NewRecorder()
		before := calls.Load()
		httpServer.Handler.ServeHTTP(rec, req)
		require.Equal(t, http.StatusOK, rec.Code, rec.Body.String())
		require.EqualValues(t, 1, calls.Load()-before, "one Auth invocation per transport operation")
		var envelope struct {
			Result json.RawMessage
			Error  any
		}
		body := rec.Body.Bytes()
		if strings.HasPrefix(rec.Header().Get("Content-Type"), "text/event-stream") {
			var data []string
			for _, line := range strings.Split(string(body), "\n") {
				if strings.HasPrefix(line, "data:") {
					data = append(data, strings.TrimSpace(strings.TrimPrefix(line, "data:")))
				}
			}
			body = []byte(strings.Join(data, "\n"))
		}
		require.NoError(t, json.Unmarshal(body, &envelope))
		require.Nil(t, envelope.Error, rec.Body.String())
		return envelope.Result
	}
	listed := rpc(t, schema.MethodToolsList, nil, "first")
	require.Contains(t, string(listed), `"id"`)
	// This proves the existing default discovery variant only. Advertising the
	// complete multi-mode union remains a separate discovery contract decision.
	for i, tc := range []struct {
		args map[string]any
		want string
	}{
		{map[string]any{}, `{"role":"first","data":[{"id":7,"writable":true}]}`},
		{map[string]any{"mode": "entries"}, `{"role":"first","data":[{"siteId":9,"name":null}]}`},
		{map[string]any{"fields": []string{"writable"}}, `{"role":"first","data":[{"writable":true}]}`},
		{map[string]any{"fields": []string{"id"}}, `{"role":"first","data":[{"id":7}]}`},
		{map[string]any{"fields": []string{}}, `{"role":"first","data":[{"id":7,"writable":true}]}`},
		{map[string]any{"fields": nil}, `{"role":"first","data":[{"id":7,"writable":true}]}`},
		{map[string]any{}, `{"role":"first","data":[{"id":7,"writable":true}]}`},
	} {
		t.Run(fmt.Sprint(i), func(t *testing.T) {
			raw := rpc(t, schema.MethodToolsCall, tc.args, "first")
			var result struct {
				IsError           bool
				StructuredContent json.RawMessage
				Content           []schema.TextContent
			}
			require.NoError(t, json.Unmarshal(raw, &result))
			require.False(t, result.IsError, string(raw))
			require.JSONEq(t, tc.want, string(result.StructuredContent))
			require.Len(t, result.Content, 1)
			require.JSONEq(t, tc.want, result.Content[0].Text)
		})
	}
	invalid := rpc(t, schema.MethodToolsCall, map[string]any{"fields": []string{"unknown"}}, "first")
	require.Contains(t, string(invalid), `"isError":true`)
	require.Contains(t, string(invalid), "unknown presentation field")
	require.Contains(t, string(invalid), `"io.viant/http-status":400`)
	second := rpc(t, schema.MethodToolsCall, map[string]any{"mode": "entries"}, "second")
	require.Contains(t, string(second), `"role":"second"`)
	// REST enters with the actual request scope and retains its original result.
	gateway := httpgateway.NewHandler(rt, nil, "presentation-rest")
	req := httptest.NewRequest("POST", "/presentation", strings.NewReader(`{"mode":"entries"}`))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Authorization", key.Sign(t, "rest", time.Now().Add(time.Hour)))
	rec := httptest.NewRecorder()
	before := calls.Load()
	gateway.ServeHTTP(rec, req)
	require.Equal(t, 200, rec.Code, rec.Body.String())
	require.JSONEq(t, `{"role":"rest","data":[{"siteId":9,"name":null}]}`, rec.Body.String())
	require.EqualValues(t, 1, calls.Load()-before)
}

func TestContractPresentationMismatchFailureReleasesHandoff(t *testing.T) {
	for _, cause := range []error{context.Canceled, context.DeadlineExceeded, errors.New("unavailable protocol source")} {
		t.Run(cause.Error(), func(t *testing.T) {
			rt, _, calls, _ := newPresentationFixture(t)
			ctx := dexec.CaptureContracts(t.Context())
			target := presentationTarget()
			initial, err := rt.ResolveContract(ctx, target, presentationScope(t, "", "", "", "first"))
			require.NoError(t, err)
			dexec.PublishContract(ctx, target, initial)
			scope := presentationScope(t, `{"mode":"entries"}`, "", "", "first")
			var sources []locator.Provider
			for _, p := range scope.Providers() {
				if p.Kind() != "query" {
					sources = append(sources, p)
				}
			}
			sources = append(sources, provider.Named("query", func(context.Context, reflect.Type, string) (any, bool, error) {
				return nil, false, &xresponse.Error{Code: 503, Cause: cause}
			}))
			_, err = rt.InvokeComponent(ctx, dexec.ComponentRequest{Target: target, Providers: sources})
			require.ErrorIs(t, err, cause)
			// A failed source proof closes the claimed operation receipt. Recovery binds
			// fresh Auth even when the host reuses its contract-capturing context.
			_, err = rt.InvokeComponent(ctx, dexec.ComponentRequest{Target: target, Providers: append([]locator.Provider{}, scope.Providers()...)})
			require.NoError(t, err)
			require.EqualValues(t, 2, calls.Load())
		})
	}
}

// Full streamable HTTP decode/metadata/binding/business/encode workload. The
// in-memory Auth fixture isolates framework work; it is not a database profile.
func BenchmarkStreamableContractPresentationHandoff(b *testing.B) {
	key := testharness.NewJWT(b)
	token := key.Sign(b, "first", time.Now().Add(time.Hour))
	rt, entries, calls, policies := newPresentationFixture(b, key)
	service, err := nativeMCP.New(nativeMCP.Config{Components: entries, Invoker: rt, Authorization: presentationAuthorization()})
	require.NoError(b, err)
	server, err := mcpserver.New(mcpserver.Config{Service: service, Transport: mcpserver.TransportConfig{Kind: mcpserver.TransportStreamable}})
	require.NoError(b, err)
	httpServer, err := server.HTTP()
	require.NoError(b, err)
	b.Cleanup(func() { require.NoError(b, server.Shutdown(context.Background())) })
	payload, err := json.Marshal(map[string]any{"jsonrpc": "2.0", "id": 1, "method": schema.MethodToolsCall, "params": map[string]any{"name": "Presentation", "arguments": map[string]any{"mode": "entries"}, "_meta": map[string]any{"io.modelcontextprotocol/protocolVersion": schema.LatestProtocolVersion, "io.modelcontextprotocol/clientCapabilities": map[string]any{}}}})
	require.NoError(b, err)
	invoke := func() {
		req := httptest.NewRequest("POST", "/mcp", bytes.NewReader(payload))
		req.Header.Set("Content-Type", "application/json")
		req.Header.Set("Accept", "application/json, text/event-stream")
		req.Header.Set(schema.HeaderProtocolVersion, schema.LatestProtocolVersion)
		req.Header.Set(schema.HeaderMethod, schema.MethodToolsCall)
		req.Header.Set(schema.HeaderName, "Presentation")
		req.Header.Set("Authorization", token)
		rec := httptest.NewRecorder()
		httpServer.Handler.ServeHTTP(rec, req)
		if rec.Code != 200 || strings.Contains(rec.Body.String(), `"isError":true`) || strings.Contains(rec.Body.String(), `"error":`) {
			b.Fatalf("%d %s", rec.Code, rec.Body.String())
		}
	}
	start := time.Now()
	invoke()
	cold := time.Since(start)
	initialAuth, initialPolicies := calls.Load(), policies.Load()
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		invoke()
	}
	b.StopTimer()
	b.ReportMetric(float64(calls.Load()-initialAuth)/float64(b.N), "auth/op")
	b.ReportMetric(float64(policies.Load()-initialPolicies)/float64(b.N), "policies/op")
	b.ReportMetric(float64(cold.Nanoseconds()), "cold-ns")
}

func presentationAuthorization() *authorization.Policy {
	return &authorization.Policy{Tools: map[string]*authorization.Authorization{"Presentation": {ProtectedResourceMetadata: &oauthmeta.ProtectedResourceMetadata{Resource: "http://presentation.test/mcp"}}}}
}
