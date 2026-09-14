package runtime

import (
	"context"
	"errors"
	"net/http"
	"net/url"
	"reflect"
	"testing"

	"github.com/viant/bindly/locator"
	"github.com/viant/datly/bootstrap"
	"github.com/viant/datly/internal/testharness"
	rhandler "github.com/viant/datly/runtime/handler"
	customhandler "github.com/viant/datly/runtime/handler/custom"
	handlerprovider "github.com/viant/datly/runtime/handler/provider"
	"github.com/viant/datly/runtime/registry"
	"github.com/viant/datly/runtime/route"
	"github.com/viant/datly/spec"
	sqldml "github.com/viant/datly/sql/dml"
	xexec "github.com/viant/xdatly/exec"
	xhandler "github.com/viant/xdatly/handler"
	xresponse "github.com/viant/xdatly/response"
)

type customRouteInput struct {
	ID          int
	Initialized bool
}

func (i *customRouteInput) Init(context.Context) error {
	i.Initialized = true
	return nil
}

type customRouteOutput struct {
	ID        int
	Tenant    string
	Finalized bool
}

func (o *customRouteOutput) Finalize(_ context.Context, err error) error {
	if err == nil {
		o.Finalized = true
	}
	return nil
}

type customRouteLogger struct{}

func (customRouteLogger) Debug(string, ...any) {}
func (customRouteLogger) Info(string, ...any)  {}
func (customRouteLogger) Warn(string, ...any)  {}
func (customRouteLogger) Error(string, ...any) {}

func TestServiceExecutesRegisteredCustomHandlerThroughUnifiedEngine(t *testing.T) {
	component := &spec.Component{
		Key:  spec.Key{Kind: spec.KindComponent, Scope: "example.com/demo/custom", Name: "Custom"},
		Name: "Custom",
		Routes: []*spec.Route{
			{Method: http.MethodPatch, Path: "/v1/api/custom/{id}"},
		},
		Parameters: []*spec.Parameter{
			{Name: "ID", Source: spec.BindSource{Kind: "path", Name: "id"}},
		},
	}
	artifact, err := bootstrap.BuildArtifact(bootstrap.ArtifactInput{
		Component:  component,
		InputType:  reflect.TypeOf(customRouteInput{}),
		OutputType: reflect.TypeOf(customRouteOutput{}),
	})
	if err != nil {
		t.Fatalf("artifact build failed: %v", err)
	}
	bundle, err := route.NewBundle([]*spec.Component{component})
	if err != nil {
		t.Fatalf("bundle build failed: %v", err)
	}
	tenantKey := xhandler.ValueKey("tenant")
	log := customRouteLogger{}
	contract := xhandler.ContractFunc[customRouteInput, customRouteOutput](func(ctx context.Context, session xhandler.Session, input *customRouteInput, output *customRouteOutput) error {
		if input.ID != 9 || !input.Initialized {
			t.Fatalf("unexpected initialized input: %#v", input)
		}
		canonical, found, err := session.Binder().Lookup(ctx, xhandler.InputKey)
		if err != nil || !found || canonical != input {
			t.Fatalf("unexpected canonical input lookup: value=%#v found=%v err=%v", canonical, found, err)
		}
		tenant, found, err := session.Binder().Lookup(ctx, tenantKey)
		if err != nil || !found || tenant != "acme" {
			t.Fatalf("unexpected tenant lookup: value=%#v found=%v err=%v", tenant, found, err)
		}
		resolvedLogger, found, err := session.Binder().Lookup(ctx, rhandler.LoggerCapabilityKey)
		if err != nil || !found || resolvedLogger != log {
			t.Fatalf("unexpected logger injection: value=%#v found=%v err=%v", resolvedLogger, found, err)
		}
		output.ID = input.ID
		output.Tenant = tenant.(string)
		session.Response().SetStatusCode(http.StatusAccepted)
		session.Response().AddMetric(&xresponse.Metric{View: "custom"})
		return nil
	})
	registered := &registry.RegisteredComponent{
		Component:  component,
		Input:      artifact.Input,
		OutputType: reflect.TypeOf(customRouteOutput{}),
		Handler:    customhandler.New[customRouteInput, customRouteOutput](contract),
		Capabilities: rhandler.InvocationCapabilities{
			Logger: log,
		},
		Providers: []locator.Provider{handlerprovider.Static(tenantKey, "acme")},
	}
	service := newTestService(t, bundle, map[string]*registry.RegisteredComponent{
		component.Key.String(): registered,
	})
	execCtx := xexec.New(xexec.WithMethod(http.MethodPatch), xexec.WithURI("/v1/api/custom/9"))
	ctx := xexec.WithContext(context.Background(), execCtx)

	actual, err := executeTestRoute(t, service, ctx, testharness.NewRequest(http.MethodPatch, "/v1/api/custom/9").WithQuery(url.Values{}))
	if err != nil {
		t.Fatalf("custom route execution failed: %v", err)
	}
	output, ok := actual.(*customRouteOutput)
	if !ok || output.ID != 9 || output.Tenant != "acme" || !output.Finalized {
		t.Fatalf("unexpected custom output: %#v", actual)
	}
	if execCtx.StatusCode != http.StatusAccepted {
		t.Fatalf("expected custom response status %d, got %d", http.StatusAccepted, execCtx.StatusCode)
	}
	if len(execCtx.Metrics) != 1 || execCtx.Metrics[0].View != "custom" {
		t.Fatalf("expected custom metric propagation, got %#v", execCtx.Metrics)
	}
}

func TestRuntimeInjectsCanonicalComponentConstants(t *testing.T) {
	type input struct {
		Vendor string
	}
	type output struct {
		Vendor string
	}
	value := "VENDOR"
	component := &spec.Component{
		Key:      spec.Key{Kind: spec.KindComponent, Scope: "example.com/demo/custom", Name: "Constants"},
		Name:     "Constants",
		Routes:   []*spec.Route{{Method: http.MethodGet, Path: "/constants"}},
		Settings: &spec.Settings{Const: map[string]string{"Vendor": value}},
		Parameters: []*spec.Parameter{{
			Name: "Vendor", Source: spec.BindSource{Kind: "const", Name: "Vendor"}, TypeExpr: "string", Value: &value,
		}},
	}
	artifact, err := bootstrap.BuildArtifact(bootstrap.ArtifactInput{
		Component: component, InputType: reflect.TypeOf(input{}), OutputType: reflect.TypeOf(output{}),
	})
	if err != nil {
		t.Fatalf("BuildArtifact() error = %v", err)
	}
	runtime, err := NewRuntime([]*registry.RegisteredComponent{{
		Component: artifact.Component, Input: artifact.Input, OutputType: reflect.TypeOf(output{}),
		Handler: customhandler.NewFunc[input, output](func(_ context.Context, actual *input) (*output, error) {
			return &output{Vendor: actual.Vendor}, nil
		}),
	}})
	if err != nil {
		t.Fatalf("NewRuntime() error = %v", err)
	}
	actual, err := executeTestRoute(t, runtime, context.Background(), testharness.NewRequest(http.MethodGet, "/constants"))
	if err != nil {
		t.Fatalf("ExecuteRoute() error = %v", err)
	}
	if result := actual.(*output); result.Vendor != value {
		t.Fatalf("output = %+v", result)
	}
}

func TestRuntimeComposesRegisteredConstantProvider(t *testing.T) {
	type input struct {
		Vendor string
		Region string
	}
	type output = input
	value := "VENDOR"
	component := &spec.Component{
		Key:    spec.Key{Kind: spec.KindComponent, Scope: "example.com/demo", Name: "Constants"},
		Routes: []*spec.Route{{Method: http.MethodGet, Path: "/constants"}},
		Parameters: []*spec.Parameter{
			{Name: "Vendor", Source: spec.BindSource{Kind: "const", Name: "Vendor"}, Value: &value},
			{Name: "Region", Source: spec.BindSource{Kind: "const", Name: "Region"}},
		},
	}
	artifact, err := bootstrap.BuildArtifact(bootstrap.ArtifactInput{Component: component, InputType: reflect.TypeOf(input{}), OutputType: reflect.TypeOf(output{})})
	if err != nil {
		t.Fatal(err)
	}
	runtime, err := NewRuntime([]*registry.RegisteredComponent{{
		Component: artifact.Component, Input: artifact.Input, OutputType: reflect.TypeOf(output{}),
		Providers: []locator.Provider{mustConstantProvider(t, map[string]string{"Vendor": "override", "Region": "registered"})},
		Handler:   customhandler.NewFunc[input, output](func(_ context.Context, actual *input) (*output, error) { return actual, nil }),
	}})
	if err != nil {
		t.Fatalf("NewRuntime() error = %v", err)
	}
	actual, err := runtime.ExecuteRoute(context.Background(), http.MethodGet, "/constants", nil)
	if err != nil || actual.(*output).Vendor != value || actual.(*output).Region != "registered" {
		t.Fatalf("ExecuteRoute() = (%+v, %v)", actual, err)
	}
}

func TestRuntimeComposesInvocationConstantProvider(t *testing.T) {
	type input struct {
		Vendor string
		Region string
	}
	type output = input
	value := "VENDOR"
	component := &spec.Component{
		Key:    spec.Key{Kind: spec.KindComponent, Scope: "example.com/demo", Name: "Constants"},
		Routes: []*spec.Route{{Method: http.MethodGet, Path: "/constants"}},
		Parameters: []*spec.Parameter{
			{Name: "Vendor", Source: spec.BindSource{Kind: "const", Name: "Vendor"}, Value: &value},
			{Name: "Region", Source: spec.BindSource{Kind: "const", Name: "Region"}},
		},
	}
	artifact, err := bootstrap.BuildArtifact(bootstrap.ArtifactInput{Component: component, InputType: reflect.TypeOf(input{}), OutputType: reflect.TypeOf(output{})})
	if err != nil {
		t.Fatalf("BuildArtifact() error = %v", err)
	}
	runtime, err := NewRuntime([]*registry.RegisteredComponent{{
		Component: artifact.Component, Input: artifact.Input, OutputType: reflect.TypeOf(output{}),
		Handler: customhandler.NewFunc[input, output](func(_ context.Context, actual *input) (*output, error) {
			return actual, nil
		}),
	}})
	if err != nil {
		t.Fatalf("NewRuntime() error = %v", err)
	}
	actual, err := runtime.ExecuteRoute(context.Background(), http.MethodGet, "/constants", nil)
	if err != nil || actual.(*output).Vendor != value {
		t.Fatalf("canonical ExecuteRoute() = (%+v, %v)", actual, err)
	}
	actual, err = runtime.ExecuteRoute(context.Background(), http.MethodGet, "/constants", runtimeProviderScope{
		mustConstantProvider(t, map[string]string{"Vendor": "override", "Region": "invocation"}),
	})
	if err != nil || actual.(*output).Vendor != value || actual.(*output).Region != "invocation" {
		t.Fatalf("ExecuteRoute() = (%+v, %v)", actual, err)
	}
}

func mustConstantProvider(t *testing.T, values map[string]string) locator.Provider {
	t.Helper()
	provider, err := handlerprovider.Constants(values)
	if err != nil {
		t.Fatalf("Constants() error = %v", err)
	}
	return provider
}

func TestServiceRejectsCustomHandlerContractMismatch(t *testing.T) {
	type wrongInput struct{}
	component := &spec.Component{
		Key:    spec.Key{Kind: spec.KindComponent, Scope: "example.com/demo/custom", Name: "Mismatch"},
		Routes: []*spec.Route{{Method: http.MethodGet, Path: "/mismatch"}},
	}
	bundle, err := route.NewBundle([]*spec.Component{component})
	if err != nil {
		t.Fatalf("bundle build failed: %v", err)
	}
	registered := &registry.RegisteredComponent{
		Component:  component,
		Input:      componentArtifact(t, component, reflect.TypeOf(customRouteInput{}), reflect.TypeOf(customRouteOutput{})).Input,
		OutputType: reflect.TypeOf(customRouteOutput{}),
		Handler: customhandler.NewFunc[wrongInput, customRouteOutput](func(context.Context, *wrongInput) (*customRouteOutput, error) {
			return &customRouteOutput{}, nil
		}),
	}
	service := newTestService(t, bundle, map[string]*registry.RegisteredComponent{component.Key.String(): registered})
	if _, err := executeTestRoute(t, service, context.Background(), testharness.NewRequest(http.MethodGet, "/mismatch")); err == nil {
		t.Fatal("expected custom handler contract mismatch")
	}
}

func TestServiceCustomHandlerUsesEngineOwnedDMLCapability(t *testing.T) {
	h := testharness.NewSQLiteHarness(t)
	ctx := context.Background()
	if err := h.ExecStatements(ctx, `CREATE TABLE events (id INTEGER PRIMARY KEY AUTOINCREMENT, name TEXT)`); err != nil {
		t.Fatalf("setup failed: %v", err)
	}
	type input struct {
		Name string
	}
	type output struct {
		Name string
	}
	type event struct {
		Name string `sqlx:"name"`
	}
	component := &spec.Component{
		Key:    spec.Key{Kind: spec.KindComponent, Scope: "example.com/demo/custom", Name: "WriteEvent"},
		Routes: []*spec.Route{{Method: http.MethodPost, Path: "/v1/api/custom/events"}},
		Parameters: []*spec.Parameter{{Name: "Name", Source: spec.BindSource{Kind: "query", Name: "name"}}},
	}
	artifact, err := bootstrap.BuildArtifact(bootstrap.ArtifactInput{Component: component, InputType: reflect.TypeOf(input{}), OutputType: reflect.TypeOf(output{})})
	if err != nil {
		t.Fatalf("artifact build failed: %v", err)
	}
	contract := xhandler.ContractFunc[input, output](func(ctx context.Context, session xhandler.Session, input *input, output *output) error {
		resolved, found, err := session.Binder().Lookup(ctx, xhandler.DMLKey)
		if err != nil || !found {
			return errors.New("DML capability missing")
		}
		if err := resolved.(xhandler.DML).Insert("events", &event{Name: input.Name}); err != nil {
			return err
		}
		output.Name = input.Name
		return nil
	})
	bundle, err := route.NewBundle([]*spec.Component{component})
	if err != nil {
		t.Fatalf("bundle build failed: %v", err)
	}
	service := newTestService(t, bundle, map[string]*registry.RegisteredComponent{
		component.Key.String(): {
			Component:  component,
			Input:      artifact.Input,
			OutputType: reflect.TypeOf(output{}),
			DataSource: sqldml.Source{DB: h.DB},
			Handler:    customhandler.New[input, output](contract),
		},
	})
	actual, err := executeTestRoute(t, service, ctx, testharness.NewRequest(http.MethodPost, "/v1/api/custom/events").WithQuery(url.Values{"name": []string{"Ada"}}))
	if err != nil {
		t.Fatalf("custom write route failed: %v", err)
	}
	if actual.(*output).Name != "Ada" {
		t.Fatalf("unexpected output: %#v", actual)
	}
	var name string
	if err := h.DB.QueryRowContext(ctx, `SELECT name FROM events`).Scan(&name); err != nil || name != "Ada" {
		t.Fatalf("expected engine-flushed custom insert, name=%q err=%v", name, err)
	}
}
