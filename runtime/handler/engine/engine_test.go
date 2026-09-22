package engine

import (
	"context"
	"errors"
	"net/url"
	"reflect"
	"testing"

	"github.com/viant/bindly"
	"github.com/viant/bindly/locator"
	bindstate "github.com/viant/bindly/state"
	"github.com/viant/datly/internal/testharness"
	rhandler "github.com/viant/datly/runtime/handler"
	handlerprovider "github.com/viant/datly/runtime/handler/provider"
	"github.com/viant/datly/runtime/registry"
	"github.com/viant/datly/spec"
	xhandler "github.com/viant/xdatly/handler"
	xmcp "github.com/viant/xdatly/handler/mcp"
)

type engineInput struct {
	ID          int
	Initialized bool
}

func testRouteInput(t *testing.T, inputType reflect.Type, bindings ...bindly.BindingSpec) *registry.RouteInputContract {
	t.Helper()
	injector, err := bindly.NewInjector()
	if err != nil {
		t.Fatal(err)
	}
	plan, err := injector.CompilePlan(inputType, bindings...)
	if err != nil {
		t.Fatal(err)
	}
	return testRouteInputWithPlan(t, inputType, plan, bindings...)
}

func testRouteInputWithPlan(t *testing.T, inputType reflect.Type, plan *bindly.Plan, bindings ...bindly.BindingSpec) *registry.RouteInputContract {
	t.Helper()
	ref := spec.RouteRef{Method: "TEST", Path: "/engine"}
	projection, err := plan.Projection()
	if err != nil {
		t.Fatal(err)
	}
	contract, err := registry.NewInputContract(inputType, projection, registry.RouteInput{Route: ref, Plan: plan, Bindings: bindings})
	if err != nil {
		t.Fatal(err)
	}
	route, ok := contract.ForRoute(ref)
	if !ok {
		t.Fatal("test route input contract was not found")
	}
	return route
}

type requestPrecedenceInput struct {
	Name string
}

type boundEngineInput struct {
	Name        string
	Initialized int
}

func (i *boundEngineInput) Init(context.Context) error {
	i.Initialized++
	return nil
}

type engineProviderScope []locator.Provider

func (s engineProviderScope) Providers() []locator.Provider { return s }

type engineValidator struct{ name string }

func (v *engineValidator) Validate(context.Context, any, ...any) (*xhandler.Validation, error) {
	return nil, nil
}

func (i *engineInput) Init(context.Context) error {
	i.Initialized = true
	return nil
}

var errEngineInputInit = errors.New("init failed")

type failingEngineInput struct{}

func (*failingEngineInput) Init(context.Context) error {
	return errEngineInputInit
}

type engineMCPContext struct{}

func (engineMCPContext) Client() xmcp.Client { return nil }

type mcpEngineInput struct {
	order      []string
	mcpContext xmcp.Context
}

func (i *mcpEngineInput) Init(context.Context) error {
	i.order = append(i.order, "init")
	return nil
}

func (i *mcpEngineInput) InitMCP(_ context.Context, value xmcp.Context) error {
	i.order = append(i.order, "mcp")
	i.mcpContext = value
	return nil
}

var errMCPInputInit = errors.New("MCP init failed")

type failingMCPInput struct{}

func (*failingMCPInput) InitMCP(context.Context, xmcp.Context) error {
	return errMCPInputInit
}

type finalizerInputContextInput struct {
	Debug bool
}

type finalizerInputContextOutput struct {
	Metrics *string
	Seen    *finalizerInputContextInput
}

func (o *finalizerInputContextOutput) Finalize(ctx context.Context) error {
	input, _ := ctx.Value(reflect.TypeOf((*finalizerInputContextInput)(nil))).(*finalizerInputContextInput)
	o.Seen = input
	if input == nil || !input.Debug {
		o.Metrics = nil
	}
	return nil
}

func TestEngineExecuteBindsOnceAndInvokesWithScopedLocator(t *testing.T) {
	injector, err := bindly.NewInjector()
	if err != nil {
		t.Fatalf("NewInjector() error = %v", err)
	}
	binding := bindly.BindingSpec{
		Path: "ID", Name: "ID", Location: bindstate.Location{Kind: "path", In: "id"},
	}
	plan, err := injector.CompilePlan(reflect.TypeOf(engineInput{}), binding)
	if err != nil {
		t.Fatalf("CompilePlan() error = %v", err)
	}
	scopedKey := xhandler.ValueKey("tenant")
	actual, err := New().Execute(context.Background(), Request{
		Injector:  injector,
		Input:     testRouteInputWithPlan(t, reflect.TypeOf(engineInput{}), plan, binding),
		Scope:     testharness.Request{}.WithPathParams(map[string]string{"id": "7"}),
		Providers: []locator.Provider{handlerprovider.Static(scopedKey, "acme")},
		Handler: rhandler.HandlerFunc(func(ctx context.Context, invocation rhandler.Invocation) (any, error) {
			input, ok := invocation.Input.(*engineInput)
			if !ok || input.ID != 7 || !input.Initialized {
				t.Fatalf("unexpected canonical input: %#v", invocation.Input)
			}
			bound, found, err := invocation.Binder.Lookup(ctx, xhandler.InputKey)
			if err != nil || !found || bound != input {
				t.Fatalf("unexpected input lookup: value=%#v found=%v err=%v", bound, found, err)
			}
			tenant, found, err := invocation.Binder.Lookup(ctx, scopedKey)
			if err != nil || !found || tenant != "acme" {
				t.Fatalf("unexpected scoped lookup: value=%#v found=%v err=%v", tenant, found, err)
			}
			return input.ID, nil
		}),
	})
	if err != nil {
		t.Fatalf("Execute failed: %v", err)
	}
	if actual != 7 {
		t.Fatalf("unexpected result: %v", actual)
	}
}

func TestEngineOutputFinalizerReceivesBoundInputContext(t *testing.T) {
	injector, err := bindly.NewInjector()
	if err != nil {
		t.Fatalf("NewInjector() error = %v", err)
	}
	binding := bindly.BindingSpec{
		Path: "Debug", Name: "debug", Location: bindstate.Location{Kind: "query", In: "debug"},
	}
	plan, err := injector.CompilePlan(reflect.TypeOf(finalizerInputContextInput{}), binding)
	if err != nil {
		t.Fatalf("CompilePlan() error = %v", err)
	}
	input := testRouteInputWithPlan(t, reflect.TypeOf(finalizerInputContextInput{}), plan, binding)

	for _, testCase := range []struct {
		name        string
		query       url.Values
		wantDebug   bool
		wantMetrics bool
	}{
		{name: "default false clears metrics", wantDebug: false, wantMetrics: false},
		{name: "explicit false clears metrics", query: url.Values{"debug": {"false"}}, wantDebug: false, wantMetrics: false},
		{name: "true preserves metrics", query: url.Values{"debug": {"true"}}, wantDebug: true, wantMetrics: true},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			value := "present"
			actual, err := New().Execute(context.Background(), Request{
				Injector: injector,
				Input:    input,
				Scope:    testharness.Request{}.WithQuery(testCase.query),
				Handler: rhandler.HandlerFunc(func(context.Context, rhandler.Invocation) (any, error) {
					return &finalizerInputContextOutput{Metrics: &value}, nil
				}),
			})
			if err != nil {
				t.Fatalf("Execute() error = %v", err)
			}
			output, ok := actual.(*finalizerInputContextOutput)
			if !ok {
				t.Fatalf("output = %T", actual)
			}
			if output.Seen == nil || output.Seen.Debug != testCase.wantDebug {
				t.Fatalf("finalizer input = %+v, want debug=%v", output.Seen, testCase.wantDebug)
			}
			if got := output.Metrics != nil; got != testCase.wantMetrics {
				t.Fatalf("metrics present = %v, want %v", got, testCase.wantMetrics)
			}
		})
	}
}

func TestEngineInputContextShadowsParentForNestedInvocation(t *testing.T) {
	parentInput := &finalizerInputContextInput{Debug: false}
	childInput := &finalizerInputContextInput{Debug: true}
	childMetric := "child"
	var parentContextInput *finalizerInputContextInput
	var childOutput *finalizerInputContextOutput

	actual, err := New().Execute(context.Background(), Request{
		Input:      testRouteInput(t, reflect.TypeOf(finalizerInputContextInput{})),
		BoundInput: parentInput,
		Handler: rhandler.HandlerFunc(func(ctx context.Context, _ rhandler.Invocation) (any, error) {
			parentContextInput, _ = ctx.Value(reflect.TypeOf((*finalizerInputContextInput)(nil))).(*finalizerInputContextInput)
			result, err := New().Execute(ctx, Request{
				Input:      testRouteInput(t, reflect.TypeOf(finalizerInputContextInput{})),
				BoundInput: childInput,
				Handler: rhandler.HandlerFunc(func(context.Context, rhandler.Invocation) (any, error) {
					return &finalizerInputContextOutput{Metrics: &childMetric}, nil
				}),
			})
			if err != nil {
				return nil, err
			}
			childOutput, _ = result.(*finalizerInputContextOutput)
			return "parent", nil
		}),
	})
	if err != nil {
		t.Fatalf("Execute() error = %v", err)
	}
	if parentContextInput != parentInput {
		t.Fatalf("parent context input = %p, want %p", parentContextInput, parentInput)
	}
	if actual != "parent" {
		t.Fatalf("parent result = %v", actual)
	}
	if childOutput == nil {
		t.Fatal("child output was not captured")
	}
	if childOutput.Seen != childInput {
		t.Fatalf("child finalizer input = %p, want %p", childOutput.Seen, childInput)
	}
	if childOutput.Metrics == nil {
		t.Fatal("child finalizer used parent input and cleared metrics")
	}
}

func TestEngineExecuteUsesTrustedBoundInputWithoutProviderLookup(t *testing.T) {
	lookupCalls := 0
	query := handlerprovider.Named("query", func(context.Context, reflect.Type, string) (any, bool, error) {
		lookupCalls++
		return "provider", true, nil
	})
	bound := &boundEngineInput{Name: "bound"}
	tenantKey := xhandler.ValueKey("tenant")
	actual, err := New().Execute(context.Background(), Request{
		Input: testRouteInput(t, reflect.TypeOf(boundEngineInput{}), bindly.BindingSpec{
			Path: "Name", Location: bindstate.Location{Kind: "query", In: "name"},
		}),
		BoundInput: bound,
		Scope:      engineProviderScope{query},
		Providers:  []locator.Provider{handlerprovider.Static(tenantKey, "acme")},
		Handler: rhandler.HandlerFunc(func(ctx context.Context, invocation rhandler.Invocation) (any, error) {
			if invocation.Input != bound || bound.Initialized != 1 || bound.Name != "bound" {
				t.Fatalf("bound input = %#v, invocation=%#v", bound, invocation.Input)
			}
			input, found, err := invocation.Binder.Lookup(ctx, xhandler.InputKey)
			if err != nil || !found || input != bound {
				t.Fatalf("input lookup = (%#v, %v, %v)", input, found, err)
			}
			tenant, found, err := invocation.Binder.Lookup(ctx, tenantKey)
			if err != nil || !found || tenant != "acme" {
				t.Fatalf("tenant lookup = (%#v, %v, %v)", tenant, found, err)
			}
			output := &struct {
				Name string `parameter:"Name,kind=input,in=Name"`
			}{}
			if err := invocation.Binder.Bind(ctx, output); err != nil || output.Name != "bound" {
				t.Fatalf("output binding = (%+v, %v)", output, err)
			}
			return invocation.Input, nil
		}),
	})
	if err != nil || actual != bound {
		t.Fatalf("Execute() = (%#v, %v)", actual, err)
	}
	if lookupCalls != 0 {
		t.Fatalf("bound input performed %d provider lookups", lookupCalls)
	}
}

func TestEngineExecuteRejectsInvalidBoundInputBeforeLifecycle(t *testing.T) {
	var typedNil *boundEngineInput
	tests := []struct {
		name  string
		input any
	}{
		{name: "value", input: boundEngineInput{}},
		{name: "typed nil", input: typedNil},
		{name: "wrong pointer", input: &engineInput{}},
	}
	for _, testCase := range tests {
		t.Run(testCase.name, func(t *testing.T) {
			invoked := false
			_, err := New().Execute(context.Background(), Request{
				Input:      testRouteInput(t, reflect.TypeOf(boundEngineInput{})),
				BoundInput: testCase.input,
				Handler: rhandler.HandlerFunc(func(context.Context, rhandler.Invocation) (any, error) {
					invoked = true
					return nil, nil
				}),
			})
			if err == nil || invoked {
				t.Fatalf("Execute() error=%v invoked=%v", err, invoked)
			}
		})
	}
}

func TestEngineRequestScopeProvidersCannotBeShadowedByRegisteredProviders(t *testing.T) {
	injector, err := bindly.NewInjector()
	if err != nil {
		t.Fatalf("NewInjector() error = %v", err)
	}
	binding := bindly.BindingSpec{
		Path: "Name", Location: bindstate.Location{Kind: "query", In: "name"},
	}
	plan, err := injector.CompilePlan(reflect.TypeOf(requestPrecedenceInput{}), binding)
	if err != nil {
		t.Fatalf("CompilePlan() error = %v", err)
	}
	registered := testharness.Request{}.WithQuery(url.Values{"name": {"registered"}})
	requestScope := testharness.Request{}.WithQuery(url.Values{"name": {"request"}})
	actual, err := New().Execute(context.Background(), Request{
		Injector:  injector,
		Input:     testRouteInputWithPlan(t, reflect.TypeOf(requestPrecedenceInput{}), plan, binding),
		Scope:     requestScope,
		Providers: registered.Providers(),
		Handler: rhandler.HandlerFunc(func(_ context.Context, invocation rhandler.Invocation) (any, error) {
			return invocation.Input.(*requestPrecedenceInput).Name, nil
		}),
	})
	if err != nil {
		t.Fatalf("Execute() error = %v", err)
	}
	if actual != "request" {
		t.Fatalf("Execute() = %q; request provider must win", actual)
	}
}

func TestEngineCapabilitiesOverrideParentOnlyWhenPresent(t *testing.T) {
	parent := &engineValidator{name: "parent"}
	invocation := &engineValidator{name: "invocation"}
	tests := []struct {
		name         string
		capabilities rhandler.InvocationCapabilities
		want         *engineValidator
	}{
		{name: "absent capability uses parent", want: parent},
		{name: "present capability shadows parent", capabilities: rhandler.InvocationCapabilities{Validator: invocation}, want: invocation},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			injector, err := bindly.NewInjector(bindly.WithProviders(handlerprovider.Static(rhandler.ValidatorCapabilityKey, parent)))
			if err != nil {
				t.Fatalf("NewInjector() error = %v", err)
			}
			actual, err := New().Execute(context.Background(), Request{
				Injector:     injector,
				Input:        testRouteInput(t, reflect.TypeOf(struct{}{})),
				Capabilities: test.capabilities,
				Handler: rhandler.HandlerFunc(func(ctx context.Context, invocation rhandler.Invocation) (any, error) {
					value, found, err := invocation.Binder.Lookup(ctx, rhandler.ValidatorCapabilityKey)
					if err != nil || !found {
						return nil, err
					}
					return value, nil
				}),
			})
			if err != nil || actual != test.want {
				t.Fatalf("Execute() = (%v, %v), want (%v, nil)", actual, err, test.want)
			}
		})
	}
}

func TestEngineExecuteStopsOnInputInitializationError(t *testing.T) {
	called := false
	_, err := New().Execute(context.Background(), Request{
		Input: testRouteInput(t, reflect.TypeOf(failingEngineInput{})),
		Handler: rhandler.HandlerFunc(func(context.Context, rhandler.Invocation) (any, error) {
			called = true
			return nil, nil
		}),
	})
	if !errors.Is(err, errEngineInputInit) {
		t.Fatalf("unexpected initializer error: %v", err)
	}
	if called {
		t.Fatal("handler must not run after initialization failure")
	}
}

func TestEngineExecuteValidatesRequest(t *testing.T) {
	if _, err := (*Engine)(nil).Execute(context.Background(), Request{}); err == nil {
		t.Fatal("expected nil engine error")
	}
	if _, err := New().Execute(context.Background(), Request{}); err == nil {
		t.Fatal("expected missing handler error")
	}
	if _, err := New().Execute(context.Background(), Request{Handler: rhandler.HandlerFunc(func(context.Context, rhandler.Invocation) (any, error) {
		return nil, nil
	})}); err == nil {
		t.Fatal("expected missing input type error")
	}
}

func TestEngineExecuteRunsMCPInitializerAfterOrdinaryInitializer(t *testing.T) {
	mcpContext := engineMCPContext{}
	var invoked bool
	result, err := New().Execute(xmcp.WithContext(context.Background(), mcpContext), Request{
		Input: testRouteInput(t, reflect.TypeOf(mcpEngineInput{})),
		Handler: rhandler.HandlerFunc(func(_ context.Context, invocation rhandler.Invocation) (any, error) {
			invoked = true
			input := invocation.Input.(*mcpEngineInput)
			if !reflect.DeepEqual(input.order, []string{"init", "mcp"}) || input.mcpContext != mcpContext {
				t.Fatalf("unexpected initializer state: %#v", input)
			}
			return input, nil
		}),
	})
	if err != nil || !invoked || result == nil {
		t.Fatalf("unexpected MCP execution: result=%#v invoked=%v err=%v", result, invoked, err)
	}
}

func TestEngineExecuteStopsAfterMCPInitializerError(t *testing.T) {
	invoked := false
	_, err := New().Execute(xmcp.WithContext(context.Background(), engineMCPContext{}), Request{
		Input: testRouteInput(t, reflect.TypeOf(failingMCPInput{})),
		Handler: rhandler.HandlerFunc(func(context.Context, rhandler.Invocation) (any, error) {
			invoked = true
			return nil, nil
		}),
	})
	if !errors.Is(err, errMCPInputInit) || invoked {
		t.Fatalf("unexpected MCP initializer failure: invoked=%v err=%v", invoked, err)
	}
}
