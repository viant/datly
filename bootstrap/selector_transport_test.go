package bootstrap

import (
	"context"
	"reflect"
	"testing"

	fixture "github.com/viant/datly/bootstrap/testdata/selectortransport"
	rhandler "github.com/viant/datly/runtime/handler"
	"github.com/viant/datly/spec"
)

func TestCustomSelectorTransportHandlerOwnedOutputIdentity(t *testing.T) {
	found, err := ReflectSelectedPackages([]string{reflect.TypeFor[fixture.Component]().PkgPath()}, nil)
	if err != nil {
		t.Fatal(err)
	}
	component, err := found.Components[0].Resolve(reflect.TypeFor[fixture.Input](), reflect.TypeFor[fixture.Output]())
	if err != nil {
		t.Fatal(err)
	}
	if component.RootView == nil {
		t.Fatal("served output view missing")
	}
	handler := selectorTransportHandler{HandlerFunc: rhandler.HandlerFunc(func(context.Context, rhandler.Invocation) (any, error) { return &fixture.Output{}, nil })}
	artifact, err := BuildArtifact(ArtifactInput{Component: component, InputType: reflect.TypeFor[fixture.Input](), OutputType: reflect.TypeFor[fixture.Output](), Handler: handler, HandlerOwnedOutput: true})
	if err != nil {
		t.Fatal(err)
	}
	if artifact.Reader != nil || artifact.Handler == nil {
		t.Fatal("handler-owned output became a reader")
	}
	route, _ := artifact.Input.ForRoute(spec.RouteRef{Method: component.Routes[0].Method, Path: component.Routes[0].Path})
	if route == nil || !route.SelectorPlan().TransportOnly() {
		t.Fatal("custom selector plan missing")
	}
	type wrongOutput struct{ Value int }
	if _, err = BuildArtifact(ArtifactInput{Component: component, InputType: reflect.TypeFor[fixture.Input](), OutputType: reflect.TypeFor[wrongOutput](), Handler: handler, HandlerOwnedOutput: true}); err == nil {
		t.Fatal("wrong actual output identity accepted")
	}
}

type selectorTransportHandler struct{ rhandler.HandlerFunc }

func (selectorTransportHandler) InputType() reflect.Type  { return reflect.TypeFor[fixture.Input]() }
func (selectorTransportHandler) OutputType() reflect.Type { return reflect.TypeFor[fixture.Output]() }
