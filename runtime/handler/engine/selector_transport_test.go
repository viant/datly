package engine

import (
	"context"
	"reflect"
	"testing"

	"github.com/viant/bindly/locator"
	"github.com/viant/datly/bootstrap"
	fixture "github.com/viant/datly/bootstrap/testdata/selectortransport"
	rhandler "github.com/viant/datly/runtime/handler"
	"github.com/viant/datly/runtime/handler/compiler"
	"github.com/viant/datly/runtime/handler/provider"
	"github.com/viant/datly/spec"
)

func TestCustomSelectorTransportDoesNotBindOrCarry(t *testing.T) {
	found, err := bootstrap.ReflectSelectedPackages([]string{reflect.TypeFor[fixture.Component]().PkgPath()}, nil)
	if err != nil {
		t.Fatal(err)
	}
	component, err := found.Components[0].Resolve(reflect.TypeFor[fixture.Input](), reflect.TypeFor[fixture.Output]())
	if err != nil {
		t.Fatal(err)
	}
	compiled, err := compiler.New(compiler.Input{Component: component, InputType: reflect.TypeFor[fixture.Input]()}).Compile()
	if err != nil {
		t.Fatal(err)
	}
	route, ok := compiled.Input.ForRoute(spec.RouteRef{Method: "GET", Path: "/selector-records"})
	if !ok || !route.SelectorPlan().TransportOnly() {
		t.Fatal("compiler transport mode missing")
	}
	for _, source := range []string{"bad", "2", ""} {
		selectorReads, entries := 0, 0
		query := provider.Named("query", func(_ context.Context, _ reflect.Type, key string) (any, bool, error) {
			if key == "id" {
				return "9", true, nil
			}
			selectorReads++
			return source, source != "", nil
		})
		_, err := New().Execute(t.Context(), Request{Input: route, Providers: []locator.Provider{query}, Handler: rhandler.HandlerFunc(func(ctx context.Context, inv rhandler.Invocation) (any, error) {
			entries++
			input := inv.Input.(*fixture.Input)
			if input.ID != 9 || input.Has == nil || !input.Has.ID {
				t.Fatal("public binding changed")
			}
			if _, err := route.SelectorPlan().Value(inv.Binder, input); err == nil {
				t.Fatal("transport selector was carried")
			}
			return &fixture.Output{}, nil
		})})
		if err != nil || entries != 1 || selectorReads != 0 {
			t.Fatalf("transport parsed selector before handler: %v entries=%d reads=%d", err, entries, selectorReads)
		}
	}
	// Current execution metadata cannot relabel an admitted custom declaration.
	component.Routes[0].Handler = "DifferentHandler"
	if _, err := compiler.New(compiler.Input{Component: component, InputType: reflect.TypeFor[fixture.Input]()}).Compile(); err == nil {
		t.Fatal("different current handler accepted")
	}
}
