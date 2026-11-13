package runtime_test

import (
	"context"
	"errors"
	"fmt"
	requestprovider "github.com/viant/bindly/provider/request"
	dexec "github.com/viant/datly/exec"
	xresponse "github.com/viant/xdatly/response"
	"net/http/httptest"
	"reflect"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"encoding/json"
	"github.com/stretchr/testify/require"
	"github.com/viant/bindly"
	"github.com/viant/bindly/locator"
	"github.com/viant/datly/bootstrap"
	httpgateway "github.com/viant/datly/gateway/http"
	nativeMCP "github.com/viant/datly/mcp"
	druntime "github.com/viant/datly/runtime"
	rhandler "github.com/viant/datly/runtime/handler"
	"github.com/viant/datly/runtime/handler/custom"
	"github.com/viant/datly/runtime/handler/provider"
	"github.com/viant/datly/runtime/registry"
	"github.com/viant/datly/spec"
	"github.com/viant/mcp-protocol/authorization"
	"github.com/viant/mcp-protocol/schema"
	"github.com/viant/xdatly/contract"
)

type reuseAuthInput struct {
	Role string `parameter:"Role,kind=header,in=Authorization,required=true"`
}
type reuseAuthOutput struct{ Role string }
type reuseBusinessInput struct {
	Auth *reuseAuthOutput `parameter:"Auth,kind=component,in=GET:/reuse-auth,required=true"`
}
type reuseFreshBusinessInput struct {
	Auth *reuseAuthOutput `parameter:"Auth,kind=component,in=GET:/reuse-auth,required=true,cacheable=false"`
}
type reuseBusinessOutput struct {
	Role string `json:"role"`
}

func (*reuseBusinessOutput) Contract(ctx context.Context, binder bindly.Binder) (*contract.Policy, error) {
	var state struct {
		Auth *reuseAuthOutput `parameter:"Auth,kind=component,in=GET:/reuse-auth,required=true"`
	}
	if err := binder.Bind(ctx, &state); err != nil {
		return nil, err
	}
	return nil, nil
}

// One transport operation must reuse its declared dependency across metadata
// and execution, while the next operation must obtain fresh dependency values.
func TestHTTPContractAndExecutionReuseDeclaredDependency(t *testing.T) {
	runtime, authCalls, businessCalls, _ := newContractReuseFixture(t)
	gateway := httpgateway.NewHandler(runtime, nil, "reuse-test")
	for _, role := range []string{"first", "second"} {
		t.Run(role, func(t *testing.T) {
			before := authCalls.Load()
			req := httptest.NewRequest("GET", "/reuse-business", nil)
			req.Header.Set("Authorization", role)
			response := httptest.NewRecorder()
			gateway.ServeHTTP(response, req)
			require.Equal(t, 200, response.Code, response.Body.String())
			require.JSONEq(t, `{"role":"`+role+`"}`, response.Body.String())
			require.EqualValues(t, 1, authCalls.Load()-before, "same request resolved its dependency more than once")
		})
	}
	require.EqualValues(t, 2, businessCalls.Load())
}

func newContractReuseFixture(t testing.TB, check ...func(context.Context, *reuseAuthInput) error) (*druntime.Runtime, *atomic.Int64, *atomic.Int64, []*registry.RegisteredComponent) {
	return newContractReuseFixtureInput(t, false, check...)
}
func newContractReuseFixtureInput(t testing.TB, fresh bool, check ...func(context.Context, *reuseAuthInput) error) (*druntime.Runtime, *atomic.Int64, *atomic.Int64, []*registry.RegisteredComponent) {
	t.Helper()
	authCalls, businessCalls := new(atomic.Int64), new(atomic.Int64)
	component := func(name, path string) *spec.Component {
		return &spec.Component{Key: spec.Key{Kind: spec.KindComponent, Scope: "example.com/reuse", Name: name}, Routes: []*spec.Route{{Method: "GET", Path: path, MCP: []*spec.MCPExposure{{Kind: spec.MCPExposureTool, Name: name}}}}}
	}
	auth, err := bootstrap.BuildArtifact(bootstrap.ArtifactInput{Component: component("auth", "/reuse-auth"), InputType: reflect.TypeFor[reuseAuthInput](), OutputType: reflect.TypeFor[reuseAuthOutput](), Handler: custom.NewFunc(func(ctx context.Context, in *reuseAuthInput) (*reuseAuthOutput, error) {
		authCalls.Add(1)
		if len(check) > 0 {
			if err := check[0](ctx, in); err != nil {
				return nil, err
			}
		}
		return &reuseAuthOutput{Role: in.Role}, nil
	})})
	require.NoError(t, err)
	businessType := reflect.TypeFor[reuseBusinessInput]()
	var businessHandler rhandler.TypedHandler = custom.NewFunc(func(_ context.Context, in *reuseBusinessInput) (*reuseBusinessOutput, error) {
		businessCalls.Add(1)
		return &reuseBusinessOutput{Role: in.Auth.Role}, nil
	})
	if fresh {
		businessType = reflect.TypeFor[reuseFreshBusinessInput]()
		businessHandler = custom.NewFunc(func(_ context.Context, in *reuseFreshBusinessInput) (*reuseBusinessOutput, error) {
			businessCalls.Add(1)
			return &reuseBusinessOutput{Role: in.Auth.Role}, nil
		})
	}
	business, err := bootstrap.BuildArtifact(bootstrap.ArtifactInput{Component: component("business", "/reuse-business"), InputType: businessType, OutputType: reflect.TypeFor[reuseBusinessOutput](), Handler: businessHandler})
	require.NoError(t, err)
	authEntry, err := auth.Registration(registry.RegisteredComponent{})
	require.NoError(t, err)
	businessEntry, err := business.Registration(registry.RegisteredComponent{})
	require.NoError(t, err)
	runtime, err := druntime.NewRuntime([]*registry.RegisteredComponent{authEntry, businessEntry})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, runtime.Shutdown(context.Background())) })

	return runtime, authCalls, businessCalls, []*registry.RegisteredComponent{authEntry, businessEntry}
}

func TestMCPContractAndExecutionReuseDeclaredDependency(t *testing.T) {
	runtime, authCalls, businessCalls, entries := newContractReuseFixture(t)
	service, err := nativeMCP.New(nativeMCP.Config{Components: entries, Invoker: runtime})
	require.NoError(t, err)
	tool, ok := service.Registry().ToolRegistry.Get("business")
	require.True(t, ok)
	for _, role := range []string{"first", "second"} {
		before := authCalls.Load()
		ctx := context.WithValue(t.Context(), authorization.TokenKey, &authorization.Token{Token: role})
		result, rpc := tool.Handler(ctx, &schema.CallToolRequest{Method: schema.MethodToolsCall, Params: schema.CallToolRequestParams{Name: "business", Arguments: map[string]any{}}})
		require.Nil(t, rpc)
		require.NotNil(t, result)
		require.False(t, result.IsError != nil && *result.IsError, "%+v", result)
		encoded, err := json.Marshal(result.StructuredContent)
		require.NoError(t, err)
		require.JSONEq(t, `{"role":"Bearer `+role+`"}`, string(encoded))
		require.EqualValues(t, 1, authCalls.Load()-before)
	}
	require.EqualValues(t, 2, businessCalls.Load())
}

func contractReuseScope(role string) *requestprovider.Scope {
	return requestprovider.NewValues(requestprovider.WithHeaders(map[string][]string{"Authorization": {role}}))
}
func contractReuseTarget() dexec.ComponentTarget {
	return dexec.ComponentTarget{Component: spec.Key{Kind: spec.KindComponent, Scope: "example.com/reuse", Name: "business"}, Route: spec.RouteRef{Method: "GET", Path: "/reuse-business"}}
}
func TestContractHandoffProviderChangeAndSingleOperation(t *testing.T) {
	runtime, calls, business, _ := newContractReuseFixture(t)
	ctx := dexec.CaptureContracts(t.Context())
	target := contractReuseTarget()
	first := contractReuseScope("first")
	defer first.Close()
	effective, err := runtime.ResolveContract(ctx, target, first)
	require.NoError(t, err)
	dexec.PublishContract(ctx, target, effective)
	require.EqualValues(t, 1, calls.Load())
	for i, role := range []string{"second", "third"} {
		scope := contractReuseScope(role)
		value, err := runtime.InvokeComponent(ctx, dexec.ComponentRequest{Target: target, Providers: scope.Providers()})
		require.NoError(t, err)
		require.NoError(t, scope.Close())
		require.Equal(t, role, value.(*reuseBusinessOutput).Role)
		require.EqualValues(t, i+2, calls.Load(), "provider overrides and subsequent operations must reauthorize")
	}
	require.EqualValues(t, 2, business.Load())
}
func TestContractHandoffCannotCrossRuntimeGeneration(t *testing.T) {
	first, firstCalls, _, _ := newContractReuseFixture(t)
	second, secondCalls, _, _ := newContractReuseFixture(t, func(_ context.Context, in *reuseAuthInput) error { in.Role = "second/" + in.Role; return nil })
	scope := contractReuseScope("caller")
	defer scope.Close()
	ctx := dexec.CaptureContracts(t.Context())
	target := contractReuseTarget()
	effective, err := first.ResolveContract(ctx, target, scope)
	require.NoError(t, err)
	dexec.PublishContract(ctx, target, effective)
	value, err := second.InvokeComponent(ctx, dexec.ComponentRequest{Target: target, Providers: scope.Providers()})
	require.NoError(t, err)
	require.Equal(t, "second/caller", value.(*reuseBusinessOutput).Role)
	require.EqualValues(t, 1, firstCalls.Load())
	require.EqualValues(t, 1, secondCalls.Load())
}
func TestContractHandoffFailureCauseAndRecovery(t *testing.T) {
	unavailable := errors.New("fixture auth unavailable")
	runtime, calls, business, _ := newContractReuseFixture(t, func(_ context.Context, in *reuseAuthInput) error {
		if in.Role == "unavailable" {
			return &xresponse.Error{Code: 503, Cause: unavailable}
		}
		return nil
	})
	scope := contractReuseScope("unavailable")
	defer scope.Close()
	_, err := runtime.InvokeComponent(t.Context(), dexec.ComponentRequest{Target: contractReuseTarget(), Providers: scope.Providers()})
	require.ErrorIs(t, err, unavailable)
	require.EqualValues(t, 503, xresponse.ErrorStatusCode(err, 500))
	require.Zero(t, business.Load())
	valid := contractReuseScope("recovered")
	defer valid.Close()
	value, err := runtime.InvokeComponent(t.Context(), dexec.ComponentRequest{Target: contractReuseTarget(), Providers: valid.Providers()})
	require.NoError(t, err)
	require.Equal(t, "recovered", value.(*reuseBusinessOutput).Role)
	require.EqualValues(t, 2, calls.Load())
	require.EqualValues(t, 1, business.Load())
	canceled, cancel := context.WithCancel(t.Context())
	cancel()
	_, err = runtime.InvokeComponent(canceled, dexec.ComponentRequest{Target: contractReuseTarget(), Providers: valid.Providers()})
	require.ErrorIs(t, err, context.Canceled)
	require.EqualValues(t, 2, calls.Load())
	require.EqualValues(t, 1, business.Load())
}
func TestHTTPContractHandoffConcurrentRequests(t *testing.T) {
	runtime, calls, business, _ := newContractReuseFixture(t)
	gateway := httpgateway.NewHandler(runtime, nil, "reuse-parallel")
	const count = 8
	type observed struct {
		role, body string
		status     int
	}
	results := make(chan observed, count)
	var group sync.WaitGroup
	for i := 0; i < count; i++ {
		group.Add(1)
		go func(i int) {
			defer group.Done()
			role := fmt.Sprintf("role-%d", i)
			request := httptest.NewRequest("GET", "/reuse-business", nil)
			request.Header.Set("Authorization", role)
			response := httptest.NewRecorder()
			gateway.ServeHTTP(response, request)
			results <- observed{role, response.Body.String(), response.Code}
		}(i)
	}
	group.Wait()
	close(results)
	for result := range results {
		require.Equal(t, 200, result.status)
		require.JSONEq(t, `{"role":"`+result.role+`"}`, result.body)
	}
	require.EqualValues(t, count, calls.Load())
	require.EqualValues(t, count, business.Load())
}

// Measures the complete HTTP metadata/binding/execution/encoding path. The
// dependency is in-memory: this isolates framework overhead, not database cost.
func BenchmarkHTTPContractBindingHandoff(b *testing.B) {
	runtime, calls, _, _ := newContractReuseFixture(b)
	gateway := httpgateway.NewHandler(runtime, nil, "reuse-benchmark")
	invoke := func() {
		request := httptest.NewRequest("GET", "/reuse-business", nil)
		request.Header.Set("Authorization", "caller")
		response := httptest.NewRecorder()
		gateway.ServeHTTP(response, request)
		if response.Code != 200 {
			b.Fatal(response.Body.String())
		}
	}
	start := time.Now()
	invoke()
	first := time.Since(start)
	initial := calls.Load()
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		invoke()
	}
	b.StopTimer()
	b.ReportMetric(float64(calls.Load()-initial)/float64(b.N), "auth/op")
	b.ReportMetric(float64(first.Nanoseconds()), "first-request-ns")
}

func TestContractHandoffHonorsNoncacheableComponentBinding(t *testing.T) {
	var counter *atomic.Int64
	runtime, calls, business, _ := newContractReuseFixtureInput(t, true, func(_ context.Context, in *reuseAuthInput) error {
		in.Role = fmt.Sprintf("%s-%d", in.Role, counter.Load())
		return nil
	})
	counter = calls
	gateway := httpgateway.NewHandler(runtime, nil, "reuse-noncacheable")
	req := httptest.NewRequest("GET", "/reuse-business", nil)
	req.Header.Set("Authorization", "caller")
	response := httptest.NewRecorder()
	gateway.ServeHTTP(response, req)
	require.Equal(t, 200, response.Code, response.Body.String())
	require.JSONEq(t, `{"role":"caller-2"}`, response.Body.String())
	require.EqualValues(t, 2, calls.Load())
	require.EqualValues(t, 1, business.Load())
}

func TestMCPContractDependencyUnavailableRemainsToolFailure(t *testing.T) {
	cause := errors.New("private backend diagnostic")
	runtime, calls, business, entries := newContractReuseFixture(t, func(_ context.Context, in *reuseAuthInput) error {
		if in.Role == "Bearer unavailable" {
			return &bindly.BindingError{Path: "Backend", Code: 503, Cause: cause}
		}
		return nil
	})
	service, err := nativeMCP.New(nativeMCP.Config{Components: entries, Invoker: runtime})
	require.NoError(t, err)
	tool, ok := service.Registry().ToolRegistry.Get("business")
	require.True(t, ok)
	for _, role := range []string{"unavailable", "unavailable", "available"} {
		ctx := context.WithValue(t.Context(), authorization.TokenKey, &authorization.Token{Token: role})
		result, rpc := tool.Handler(ctx, &schema.CallToolRequest{Method: schema.MethodToolsCall, Params: schema.CallToolRequestParams{Name: "business", Arguments: map[string]any{}}})
		require.Nil(t, rpc)
		require.NotNil(t, result)
		raw, err := json.Marshal(result.StructuredContent)
		require.NoError(t, err)
		if role == "unavailable" {
			require.True(t, result.IsError != nil && *result.IsError)
			var body map[string]any
			require.NoError(t, json.Unmarshal(raw, &body))
			require.EqualValues(t, 503, body["status"])
			require.Equal(t, "Service Unavailable", body["message"])
			require.NotContains(t, string(raw), cause.Error())
			require.Zero(t, business.Load())
		} else {
			require.False(t, result.IsError != nil && *result.IsError)
			require.JSONEq(t, `{"role":"Bearer available"}`, string(raw))
		}
	}
	require.EqualValues(t, 3, calls.Load())
	require.EqualValues(t, 1, business.Load())
}

func TestContractHandoffRejectsChangingSourcesDuringBinding(t *testing.T) {
	runtime, calls, business, _ := newContractReuseFixture(t)
	var reads atomic.Int64
	source := provider.Named("header", func(context.Context, reflect.Type, string) (any, bool, error) {
		return fmt.Sprintf("role-%d", reads.Add(1)), true, nil
	})
	_, err := runtime.InvokeComponent(t.Context(), dexec.ComponentRequest{Target: contractReuseTarget(), Providers: []locator.Provider{source}})
	require.Error(t, err)
	require.EqualValues(t, 409, xresponse.ErrorStatusCode(err, 500))
	require.EqualValues(t, 1, calls.Load())
	require.Zero(t, business.Load())
}
