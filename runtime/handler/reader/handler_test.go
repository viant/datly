package reader

import (
	"context"
	"reflect"
	"testing"

	"github.com/viant/bindly"
	rhandler "github.com/viant/datly/runtime/handler"
	"github.com/viant/datly/runtime/registry"
	"github.com/viant/datly/spec"
	"github.com/viant/sqlx"
	xhandler "github.com/viant/xdatly/handler"
)

type trackingReader struct {
	input    any
	binder   xhandler.Binder
	resolver sqlx.ParameterResolver
	result   any
}

func (r *trackingReader) Read(_ context.Context, input any, binder xhandler.Binder, resolver sqlx.ParameterResolver) (any, error) {
	r.input = input
	r.binder = binder
	r.resolver = resolver
	return r.result, nil
}

func TestHandlerDelegatesBoundInvocationToReader(t *testing.T) {
	input := &struct{ ID int }{ID: 7}
	result := &struct{ Count int }{Count: 1}
	reader := &trackingReader{result: result}

	injector, err := bindly.NewInjector()
	if err != nil {
		t.Fatal(err)
	}
	plan, err := injector.CompilePlan(reflect.TypeOf(*input))
	if err != nil {
		t.Fatal(err)
	}
	projection, err := plan.Projection(bindly.ProjectionField{Path: "ID"})
	if err != nil {
		t.Fatal(err)
	}
	contract, err := registry.NewInputContract(reflect.TypeOf(*input), projection, registry.RouteInput{
		Route: spec.RouteRef{Method: "GET", Path: "/items"}, Plan: plan,
	})
	if err != nil {
		t.Fatal(err)
	}
	route, ok := contract.ForRoute(spec.RouteRef{Method: "GET", Path: "/items"})
	if !ok {
		t.Fatal("route input contract was not found")
	}
	actual, err := NewHandler(reader, route).Execute(context.Background(), rhandler.Invocation{Input: input})
	if err != nil {
		t.Fatalf("execute failed: %v", err)
	}
	if actual != result || reader.input != input {
		t.Fatalf("unexpected delegation: result=%#v input=%#v", actual, reader.input)
	}
	value, ok, err := reader.resolver("ID")
	if err != nil || !ok || value != 7 {
		t.Fatalf("resolver(ID) = %#v, %v, %v", value, ok, err)
	}
}

func TestHandlerRejectsMissingReader(t *testing.T) {
	if _, err := NewHandler(nil, nil).Execute(context.Background(), rhandler.Invocation{}); err == nil {
		t.Fatal("expected missing reader error")
	}
}
