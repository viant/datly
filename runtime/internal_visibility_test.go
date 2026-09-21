package runtime

import (
	"context"
	"net/http"
	"reflect"
	"strings"
	"testing"

	"github.com/viant/datly/exec"
	"github.com/viant/datly/internal/testharness"
	"github.com/viant/datly/runtime/handler/custom"
	"github.com/viant/datly/runtime/registry"
	"github.com/viant/datly/spec"
	"github.com/viant/sqlx"
	xhandler "github.com/viant/xdatly/handler"
)

type internalVisibilityInput struct {
	Name string
}

type internalVisibilityOutput struct {
	Message string
}

func TestRuntimeInternalRouteHiddenFromHTTPButAvailableToInternalInvocation(t *testing.T) {
	component := componentSpec("InternalOnly", http.MethodGet, "/internal-only", []*spec.Parameter{{
		Name: "Name", Source: spec.BindSource{Kind: "query", Name: "name"}, TypeExpr: "string",
	}})
	component.Routes[0].Internal = true
	artifact := componentArtifact(t, component, reflect.TypeOf(internalVisibilityInput{}), reflect.TypeOf(internalVisibilityOutput{}))
	rt, err := NewRuntime([]*registry.RegisteredComponent{{
		Component: artifact.Component, Input: artifact.Input, OutputType: reflect.TypeOf(internalVisibilityOutput{}),
		Handler: custom.NewFunc[internalVisibilityInput, internalVisibilityOutput](func(_ context.Context, input *internalVisibilityInput) (*internalVisibilityOutput, error) {
			return &internalVisibilityOutput{Message: "hello " + input.Name}, nil
		}),
	}})
	if err != nil {
		t.Fatal(err)
	}
	if rt.ExposesComponent(component.Key) {
		t.Fatal("internal-only component is publicly exposed")
	}
	if len(rt.Routes()) != 0 {
		t.Fatalf("public routes = %+v", rt.Routes())
	}
	_, err = executeTestRoute(t, rt, context.Background(), testharness.NewRequest(http.MethodGet, "/internal-only").WithQuery(map[string][]string{"name": {"Ada"}}))
	if err == nil || !strings.Contains(err.Error(), "route not found") {
		t.Fatalf("ExecuteRoute() error = %v, want route not found", err)
	}
	actual, err := rt.InvokeComponent(context.Background(), exec.ComponentRequest{
		Target: exec.ComponentTarget{Component: component.Key, Route: spec.RouteRef{Method: http.MethodGet, Path: "/internal-only"}},
		Input:  &internalVisibilityInput{Name: "Ada"},
	})
	if err != nil {
		t.Fatalf("InvokeComponent() error = %v", err)
	}
	if output := actual.(*internalVisibilityOutput); output.Message != "hello Ada" {
		t.Fatalf("output = %+v", output)
	}
}

type internalWarmupReader struct{}

func (internalWarmupReader) Read(context.Context, any, xhandler.Binder, sqlx.ParameterResolver) (any, error) {
	return &internalVisibilityOutput{}, nil
}

func (internalWarmupReader) Warmup(context.Context, exec.ReaderWarmupInvocation) (int, error) {
	return 1, nil
}

func (internalWarmupReader) WarmupTargets() []exec.ReaderWarmupTarget {
	return []exec.ReaderWarmupTarget{{Settings: &spec.CacheWarmupSettings{}}}
}

func TestRuntimeInternalRouteStillEligibleForWarmup(t *testing.T) {
	component := componentSpec("InternalWarmup", http.MethodGet, "/internal-warmup", nil)
	component.Routes[0].Internal = true
	artifact := componentArtifact(t, component, reflect.TypeOf(struct{}{}), reflect.TypeOf(internalVisibilityOutput{}))
	rt, err := NewRuntime([]*registry.RegisteredComponent{{
		Component: artifact.Component, Input: artifact.Input, OutputType: reflect.TypeOf(internalVisibilityOutput{}), Reader: internalWarmupReader{},
	}})
	if err != nil {
		t.Fatal(err)
	}
	if len(rt.Routes()) != 0 {
		t.Fatalf("public routes = %+v", rt.Routes())
	}
	target, ok := rt.WarmupTarget("/internal-warmup")
	if !ok {
		t.Fatal("internal warmup target was not resolved")
	}
	if target.Component != component.Key || target.Route.Path != "/internal-warmup" {
		t.Fatalf("warmup target = %+v", target)
	}
	routes := rt.WarmupRoutes()
	if len(routes) != 1 || routes[0].Path != "/internal-warmup" || !routes[0].Internal {
		t.Fatalf("warmup routes = %+v", routes)
	}
}

type delegatedWarmupReader struct {
	warmups int
}

func (r *delegatedWarmupReader) Read(context.Context, any, xhandler.Binder, sqlx.ParameterResolver) (any, error) {
	return &internalVisibilityOutput{}, nil
}

func (r *delegatedWarmupReader) Warmup(context.Context, exec.ReaderWarmupInvocation) (int, error) {
	r.warmups++
	return 1, nil
}

func (r *delegatedWarmupReader) WarmupTargets() []exec.ReaderWarmupTarget {
	return []exec.ReaderWarmupTarget{{Settings: &spec.CacheWarmupSettings{}}}
}

func TestRuntimeWarmupDelegatesHandlerRouteToPrivateReader(t *testing.T) {
	handlerComponent := componentSpec("AdConfig", http.MethodGet, "/ad-config", nil)
	target := spec.RouteRef{Method: http.MethodGet, Path: "/ad-config/read"}
	handlerComponent.Settings = &spec.Settings{WarmupTarget: &target}
	handlerArtifact := componentArtifact(t, handlerComponent, reflect.TypeOf(struct{}{}), reflect.TypeOf(internalVisibilityOutput{}))
	readerComponent := componentSpec("AdConfigRead", http.MethodGet, "/ad-config/read", nil)
	readerComponent.Routes[0].Internal = true
	readerArtifact := componentArtifact(t, readerComponent, reflect.TypeOf(struct{}{}), reflect.TypeOf(internalVisibilityOutput{}))
	handlerCalls := 0
	reader := &delegatedWarmupReader{}
	rt, err := NewRuntime([]*registry.RegisteredComponent{
		{
			Component: handlerArtifact.Component, Input: handlerArtifact.Input, OutputType: reflect.TypeOf(internalVisibilityOutput{}),
			Handler: custom.NewFunc[struct{}, internalVisibilityOutput](func(context.Context, *struct{}) (*internalVisibilityOutput, error) {
				handlerCalls++
				return &internalVisibilityOutput{Message: "handler"}, nil
			}),
		},
		{
			Component: readerArtifact.Component, Input: readerArtifact.Input, OutputType: reflect.TypeOf(internalVisibilityOutput{}),
			Reader: reader,
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	warmupTarget, ok := rt.WarmupTarget("/ad-config")
	if !ok {
		t.Fatal("delegated warmup target was not resolved")
	}
	if warmupTarget.Component != readerComponent.Key || warmupTarget.Route.Path != "/ad-config/read" {
		t.Fatalf("warmup target = %+v", warmupTarget)
	}
	if _, ok = rt.WarmupTarget("/ad-config/read"); !ok {
		t.Fatal("private reader warmup target was not resolved")
	}
	if _, err = rt.Warmup(context.Background(), warmupTarget); err != nil {
		t.Fatalf("Warmup() error = %v", err)
	}
	if handlerCalls != 0 {
		t.Fatalf("handler executed during delegated warmup: %d", handlerCalls)
	}
	if reader.warmups != 1 {
		t.Fatalf("reader warmups = %d, want 1", reader.warmups)
	}
	if len(rt.Routes()) != 1 || rt.Routes()[0].Path != "/ad-config" {
		t.Fatalf("public routes = %+v", rt.Routes())
	}
}

type internalVisibilityLoader struct {
	component *registry.RegisteredComponent
}

func (l internalVisibilityLoader) LoadComponent(_ context.Context, key spec.Key) (*registry.RegisteredComponent, error) {
	if l.component != nil && l.component.Component != nil && l.component.Component.Key == key {
		return l.component, nil
	}
	return nil, nil
}

func TestIndexedRuntimeInternalVisibilityMatchesEagerRuntime(t *testing.T) {
	component := componentSpec("IndexedInternal", http.MethodGet, "/indexed-internal", nil)
	component.Routes[0].Internal = true
	artifact := componentArtifact(t, component, reflect.TypeOf(struct{}{}), reflect.TypeOf(internalVisibilityOutput{}))
	registered := &registry.RegisteredComponent{
		Component: artifact.Component, Input: artifact.Input, OutputType: reflect.TypeOf(internalVisibilityOutput{}),
		Handler: custom.NewFunc[struct{}, internalVisibilityOutput](func(context.Context, *struct{}) (*internalVisibilityOutput, error) {
			return &internalVisibilityOutput{Message: "indexed"}, nil
		}),
	}
	rt, err := NewIndexedRuntime([]*spec.Component{artifact.Component}, []*registry.RegisteredComponent{registered}, internalVisibilityLoader{component: registered})
	if err != nil {
		t.Fatal(err)
	}
	if rt.ExposesComponent(component.Key) || len(rt.Routes()) != 0 {
		t.Fatalf("indexed internal route was exposed: exposes=%t routes=%+v", rt.ExposesComponent(component.Key), rt.Routes())
	}
	_, err = rt.ExecuteRoute(context.Background(), http.MethodGet, "/indexed-internal", nil)
	if err == nil || !strings.Contains(err.Error(), "route not found") {
		t.Fatalf("ExecuteRoute() error = %v, want route not found", err)
	}
	actual, err := rt.InvokeComponent(context.Background(), exec.ComponentRequest{
		Target: exec.ComponentTarget{Component: component.Key, Route: spec.RouteRef{Method: http.MethodGet, Path: "/indexed-internal"}},
		Input:  &struct{}{},
	})
	if err != nil {
		t.Fatalf("InvokeComponent() error = %v", err)
	}
	if output := actual.(*internalVisibilityOutput); output.Message != "indexed" {
		t.Fatalf("output = %+v", output)
	}
}
