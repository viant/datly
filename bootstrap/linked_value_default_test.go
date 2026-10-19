package bootstrap

import (
	"context"
	"reflect"
	"testing"

	"github.com/viant/bindly"
	"github.com/viant/bindly/provider/values"
	"github.com/viant/datly/constant"
	"github.com/viant/datly/spec"
	dtag "github.com/viant/datly/tag"
)

func TestLinkedValueTagConstantFallbackAndOverride(t *testing.T) {
	type form struct {
		Root string `parameter:",kind=const,in=storageRoot" value:"/default/root"`
	}
	type input struct{ form }
	type output struct{}
	for _, tc := range []struct {
		name      string
		constants map[string]string
		want      string
	}{{"missing", nil, "/default/root"}, {"override", map[string]string{"storageRoot": "/configured/root"}, "/configured/root"}, {"explicit empty", map[string]string{"storageRoot": ""}, ""}} {
		t.Run(tc.name, func(t *testing.T) {
			source := &RouteSource{PackagePath: "example.com/defaults", Tag: dtag.Component{Name: "Defaults", Path: "/defaults", Method: "GET"}}
			component, err := source.Resolve(reflect.TypeFor[input](), reflect.TypeFor[output]())
			if err != nil {
				t.Fatal(err)
			}
			c, err := constant.New(tc.constants)
			if err != nil {
				t.Fatal(err)
			}
			artifact, err := BuildArtifact(ArtifactInput{Component: component, InputType: reflect.TypeFor[input](), OutputType: reflect.TypeFor[output](), Const: c})
			if err != nil {
				t.Fatal(err)
			}
			route, ok := artifact.Input.ForRoute(spec.RouteRef{Method: "GET", Path: "/defaults"})
			if !ok || len(route.Fields()) != 1 {
				t.Fatal("missing linked field")
			}
			binding := route.Fields()[0].Binding()
			injector, err := bindly.NewInjector(bindly.WithProviders(values.New("const", map[string]any{})))
			if err != nil {
				t.Fatal(err)
			}
			plan, err := injector.CompilePlan(reflect.TypeFor[input](), binding)
			if err != nil {
				t.Fatal(err)
			}
			var actual input
			if err = injector.Bind(context.Background(), &actual, bindly.WithPlan(plan)); err != nil {
				t.Fatal(err)
			}
			if actual.Root != tc.want {
				t.Fatalf("got %q want %q", actual.Root, tc.want)
			}
		})
	}
}
