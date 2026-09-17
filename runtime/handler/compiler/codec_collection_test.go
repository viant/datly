package compiler

import (
	"context"
	"net/url"
	"reflect"
	"testing"

	"github.com/viant/bindly"
	"github.com/viant/datly/internal/testharness"
	"github.com/viant/datly/spec"
)

func TestTransportCodecPreservesDeclaredStringCollection(t *testing.T) {
	type input struct{ Sort []string }
	component := &spec.Component{
		Routes:     []*spec.Route{{Method: "GET", Path: "/items"}},
		Parameters: []*spec.Parameter{{Name: "Sort", Source: spec.BindSource{Kind: "query", Name: "sort"}, TypeExpr: "[]string", Codec: &spec.Codec{Body: "Identity"}}},
	}
	compiled, err := New(Input{Component: component, InputType: reflect.TypeFor[input](), CodecFactory: &configCodecFactory{}}).Compile()
	if err != nil {
		t.Fatal(err)
	}
	route, ok := compiled.Input.ForRoute(spec.RouteRef{Method: "GET", Path: "/items"})
	if !ok {
		t.Fatal("missing route input")
	}
	for _, values := range [][]string{{"id:asc", "name:desc"}, {"id:asc"}, {""}} {
		injector, err := bindly.NewInjector()
		if err != nil {
			t.Fatal(err)
		}
		scoped, err := injector.ForScope(testharness.Request{}.WithQuery(url.Values{"sort": values}).Providers()...)
		if err != nil {
			t.Fatal(err)
		}
		actual := &input{}
		if err := scoped.Bind(context.Background(), actual, bindly.WithPlan(route.Plan())); err != nil {
			t.Fatal(err)
		}
		if !reflect.DeepEqual(values, actual.Sort) {
			t.Fatalf("got %#v, want %#v", actual.Sort, values)
		}
	}
}
