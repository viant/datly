package compiler

import (
	"context"
	"github.com/viant/bindly"
	requestprovider "github.com/viant/bindly/provider/request"
	"github.com/viant/datly/spec"
	"net/url"
	"reflect"
	"testing"
)

type choiceOnlyInput struct {
	Mode int `parameter:",kind=query,in=mode,value=4" choice:"3" choice:"1" choice:"2"`
}
type choiceValidationInput struct {
	Mode int `parameter:",kind=query,in=mode,value=4" choice:"3" choice:"1" choice:"2" validate:"choice(3,1,2)"`
}

func TestChoiceMetadataDoesNotInstallRuntimeValidation(t *testing.T) {
	for _, typ := range []reflect.Type{reflect.TypeFor[choiceOnlyInput](), reflect.TypeFor[choiceValidationInput]()} {
		compiled, e := New(Input{Component: &spec.Component{Routes: []*spec.Route{{Method: "GET", Path: "/choices"}}}, InputType: typ}).Compile()
		if e != nil {
			t.Fatal(e)
		}
		route, ok := compiled.Input.ForRoute(spec.RouteRef{Method: "GET", Path: "/choices"})
		if !ok {
			t.Fatal("route")
		}
		values := requestprovider.NewValues(requestprovider.WithQuery(url.Values{"mode": {"4"}}))
		injector, e := bindly.NewInjector()
		if e != nil {
			t.Fatal(e)
		}
		scope, e := injector.ForScope(values.Providers()...)
		if e != nil {
			t.Fatal(e)
		}
		out := reflect.New(typ)
		if e = scope.Bind(context.Background(), out.Interface(), bindly.WithPlan(route.Plan())); e != nil {
			t.Fatalf("metadata added binding rejection: %v", e)
		}
		if out.Elem().FieldByName("Mode").Int() != 4 {
			t.Fatal("native binding changed")
		}
		absent := requestprovider.NewValues(requestprovider.WithQuery(url.Values{}))
		emptyScope, e := injector.ForScope(absent.Providers()...)
		if e != nil {
			t.Fatal(e)
		}
		defaulted := reflect.New(typ)
		if e = emptyScope.Bind(context.Background(), defaulted.Interface(), bindly.WithPlan(route.Plan())); e != nil || defaulted.Elem().FieldByName("Mode").Int() != 4 {
			t.Fatalf("metadata changed default outside choices: %v", e)
		}
	}
}

func TestChoiceOptionalPointerPreservesPresence(t *testing.T) {
	type marker struct{ Mode bool }
	type input struct {
		Mode *int    `parameter:",kind=query,in=mode,required=false" choice:"3" choice:"1" choice:"2"`
		Has  *marker `setMarker:"true"`
	}
	compiled, e := New(Input{Component: &spec.Component{Routes: []*spec.Route{{Method: "GET", Path: "/optional"}}}, InputType: reflect.TypeFor[input]()}).Compile()
	if e != nil {
		t.Fatal(e)
	}
	route, _ := compiled.Input.ForRoute(spec.RouteRef{Method: "GET", Path: "/optional"})
	for _, query := range []url.Values{{}, {"mode": {"0"}}} {
		p := requestprovider.NewValues(requestprovider.WithQuery(query))
		injector, _ := bindly.NewInjector()
		scope, e := injector.ForScope(p.Providers()...)
		if e != nil {
			t.Fatal(e)
		}
		var out input
		if e = scope.Bind(context.Background(), &out, bindly.WithPlan(route.Plan())); e != nil {
			t.Fatal(e)
		}
		if len(query) == 0 {
			if out.Mode != nil || out.Has != nil && out.Has.Mode {
				t.Fatal("omission changed")
			}
		} else if out.Mode == nil || *out.Mode != 0 || out.Has == nil || !out.Has.Mode {
			t.Fatalf("explicit zero/presence changed: %+v", out)
		}
	}
}
