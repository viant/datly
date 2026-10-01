package compiler

import (
	"context"
	"errors"
	"github.com/viant/bindly"
	requestprovider "github.com/viant/bindly/provider/request"
	"github.com/viant/datly/spec"
	"net/url"
	"reflect"
	"testing"
)

func TestDefaultQueryCSVAndRepeatedLists(t *testing.T) {
	type input struct {
		Labels []string `parameter:",kind=query,in=labels"`
		IDs    []int    `parameter:",kind=query,in=id"`
		Plain  string   `parameter:",kind=query,in=plain"`
	}
	compiled, err := New(Input{Component: &spec.Component{Routes: []*spec.Route{{Method: "GET", Path: "/lists"}}}, InputType: reflect.TypeFor[input]()}).Compile()
	if err != nil {
		t.Fatal(err)
	}
	route, ok := compiled.Input.ForRoute(spec.RouteRef{Method: "GET", Path: "/lists"})
	if !ok {
		t.Fatal("route")
	}
	for _, tc := range []struct {
		name    string
		values  []string
		want    []int
		invalid bool
	}{{"CSV", []string{"1001,1002"}, []int{1001, 1002}, false}, {"repeated", []string{"1001", "1002"}, []int{1001, 1002}, false}, {"mixed keeps occurrence order", []string{"3,1", "2", "1,4"}, []int{3, 1, 2, 1, 4}, false}, {"absent", nil, nil, false}, {"empty", []string{""}, nil, true}, {"empty token", []string{"1,,2"}, nil, true}, {"invalid token", []string{"1,no,3"}, nil, true}, {"overflow", []string{"999999999999999999999999"}, nil, true}} {
		t.Run(tc.name, func(t *testing.T) {
			values := url.Values{"plain": {"a,b"}, "labels": {"a,b", "c"}}
			if tc.values != nil {
				values["id"] = tc.values
			}
			providers := requestprovider.NewValues(requestprovider.WithQuery(values))
			injector, err := bindly.NewInjector()
			if err != nil {
				t.Fatal(err)
			}
			scope, err := injector.ForScope(providers.Providers()...)
			if err != nil {
				t.Fatal(err)
			}
			var out input
			err = scope.Bind(context.Background(), &out, bindly.WithPlan(route.Plan()))
			if tc.invalid {
				var binding *bindly.BindingError
				if !errors.As(err, &binding) || binding.StatusCode() != 400 {
					t.Fatalf("error%v", err)
				}
				return
			}
			if err != nil || !reflect.DeepEqual(out.IDs, tc.want) || out.Plain != "a,b" || !reflect.DeepEqual(out.Labels, []string{"a", "b", "c"}) {
				t.Fatalf("out%+v err%v", out, err)
			}
		})
	}
}
func TestQueryCSVTypedArrayAndMetadataRestrictions(t *testing.T) {
	transform := &queryListTransformer{target: reflect.TypeFor[[]int]()}
	input := []int{2, 1, 2}
	out, err := transform.Transform(context.Background(), nil, input)
	if err != nil || !reflect.DeepEqual(out, input) {
		t.Fatalf("typed array%v err%v", out, err)
	}
	for _, tc := range []struct {
		field reflect.StructField
		kind  string
	}{{reflect.StructField{Name: "IDs", Type: reflect.TypeFor[[]int](), Tag: `queryList:"bad"`}, "query"}, {reflect.StructField{Name: "ID", Type: reflect.TypeFor[string](), Tag: `queryList:"csv"`}, "query"}, {reflect.StructField{Name: "IDs", Type: reflect.TypeFor[[]int](), Tag: `queryList:"csv"`}, "body"}} {
		binding := bindly.BindingSpec{}
		binding.Location.Kind = tc.kind
		if err := applyQueryList(tc.field, false, &binding); err == nil {
			t.Fatal("invalid list metadata accepted")
		}
	}
}

func TestDefaultQueryPrimitiveScalar(t *testing.T) {
	for _, raw := range []any{1001, float64(1001)} {
		out, err := (&queryListTransformer{target: reflect.TypeFor[[]int]()}).Transform(context.Background(), nil, raw)
		if err != nil || !reflect.DeepEqual(out, []int{1001}) {
			t.Fatalf("scalar %T: %v, %v", raw, out, err)
		}
	}
}
func TestDefaultQueryListPreservesComplexShapesAndCodecs(t *testing.T) {
	for _, typ := range []reflect.Type{reflect.TypeFor[[]struct{ ID int }](), reflect.TypeFor[[]*int](), reflect.TypeFor[[]map[string]int](), reflect.TypeFor[string]()} {
		binding := bindly.BindingSpec{}
		binding.Location.Kind = "query"
		if err := applyQueryList(reflect.StructField{Name: "Value", Type: typ}, false, &binding); err != nil || binding.Transformer != nil {
			t.Fatalf("%v modified: %v", typ, err)
		}
	}
}

func TestExplicitOptionalQueryListEmptyValue(t *testing.T) {
	optional, required := false, true
	for _, item := range []struct {
		name     string
		required *bool
		raw      []string
		invalid  bool
	}{
		{"optional empty", &optional, []string{""}, false},
		{"unspecified empty", nil, []string{""}, true},
		{"required empty", &required, []string{""}, true},
		{"optional CSV hole", &optional, []string{"1,,2"}, true},
		{"optional mixed empty", &optional, []string{"", "1"}, true},
		{"optional repeated empty", &optional, []string{"", ""}, true},
		{"optional whitespace", &optional, []string{" "}, true},
	} {
		t.Run(item.name, func(t *testing.T) {
			binding := bindly.BindingSpec{Required: item.required}
			binding.Location.Kind = "query"
			if err := applyQueryList(reflect.StructField{Name: "IDs", Type: reflect.TypeFor[[]int]()}, false, &binding); err != nil {
				t.Fatal(err)
			}
			got, err := binding.Transformer.Transform(context.Background(), nil, item.raw)
			if item.invalid {
				if err == nil {
					t.Fatalf("invalid list accepted: %v", got)
				}
				return
			}
			ids, ok := got.([]int)
			if err != nil || !ok || ids == nil || len(ids) != 0 {
				t.Fatalf("empty typed list: %T %v err%v", got, got, err)
			}
		})
	}
}
