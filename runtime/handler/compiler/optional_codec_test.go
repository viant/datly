package compiler

import (
	"context"
	"net/http"
	"net/url"
	"reflect"
	"testing"

	"github.com/viant/bindly"
	"github.com/viant/datly/internal/testharness"
	"github.com/viant/datly/spec"
)

func TestOptionalCodecRetainsTransportPresence(t *testing.T) {
	type input struct{ IDs []int }
	for _, kind := range []string{"query", "header"} {
		for _, required := range []bool{false, true} {
			for _, value := range []string{"", "17", "invalid"} {
				t.Run(kind+"/"+map[bool]string{false: "optional", true: "required"}[required]+"/"+value, func(t *testing.T) {
					codec := &transportCodec{}
					component := &spec.Component{Routes: []*spec.Route{{Method: "GET", Path: "/items"}}, Parameters: []*spec.Parameter{{Name: "IDs", Source: spec.BindSource{Kind: kind, Name: "ids"}, TypeExpr: "string", Required: &required, Codec: &spec.Codec{Body: "IDs"}}}}
					compiled, err := New(Input{Component: component, InputType: reflect.TypeFor[input](), CodecFactory: &transportCodecFactory{codec: codec}}).Compile()
					if err != nil {
						t.Fatal(err)
					}
					route, ok := compiled.Input.ForRoute(spec.RouteRef{Method: "GET", Path: "/items"})
					if !ok {
						t.Fatal("route missing")
					}
					request := testharness.NewRequest("GET", "/items")
					if value != "" {
						if kind == "query" {
							request = request.WithQuery(url.Values{"ids": {value}})
						} else {
							request = request.WithHeaders(http.Header{"Ids": {value}})
						}
					}
					injector, err := bindly.NewInjector()
					if err != nil {
						t.Fatal(err)
					}
					scoped, err := injector.ForScope(request.Providers()...)
					if err != nil {
						t.Fatal(err)
					}
					actual := &input{}
					err = scoped.Bind(context.Background(), actual, bindly.WithPlan(route.Plan()))
					wantError := value == "invalid" || value == "" && required
					if (err != nil) != wantError {
						t.Fatalf("binding error=%v, wantError=%v", err, wantError)
					}
					wantCalls := 1
					if value == "" {
						wantCalls = 0
					}
					if codec.calls != wantCalls {
						t.Fatalf("codec called %d times; want %d", codec.calls, wantCalls)
					}
					if value == "17" && !reflect.DeepEqual(actual.IDs, []int{17}) {
						t.Fatalf("decoded %#v", actual.IDs)
					}
					if value == "" && actual.IDs != nil {
						t.Fatalf("absent optional value materialized %#v", actual.IDs)
					}
				})
			}
		}
	}
}
