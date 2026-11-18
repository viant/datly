package compiler

import (
	"reflect"
	"strings"
	"testing"

	"github.com/viant/bindly"
	"github.com/viant/bindly/locator"
	"github.com/viant/datly/runtime/handler/provider"
	"github.com/viant/datly/spec"
)

func TestDerivedIdentityBindingPreservesSeededState(t *testing.T) {
	type presence struct{ Mode, OrderIds bool }
	type input struct {
		Mode     string
		OrderIds []int
		Has      *presence `setMarker:"true" json:"-"`
	}
	for _, kind := range []string{"param", "input"} {
		t.Run(kind, func(t *testing.T) {
			component := &spec.Component{Routes: []*spec.Route{{Method: "POST", Path: "/identity"}}, Parameters: []*spec.Parameter{
				{Name: "Mode", Source: spec.BindSource{Kind: kind, Name: "Mode"}},
				{Name: "OrderIds", Source: spec.BindSource{Kind: kind, Name: "OrderIds"}},
			}}
			compiled, err := New(Input{Component: component, InputType: reflect.TypeFor[input]()}).Compile()
			if err != nil {
				t.Fatal(err)
			}
			route, ok := compiled.Input.ForRoute(spec.RouteRef{Method: "POST", Path: "/identity"})
			if !ok {
				t.Fatal("missing native route")
			}
			injector, err := bindly.NewInjector(bindly.WithProviders(provider.Parameter(), provider.Input()))
			if err != nil {
				t.Fatal(err)
			}
			for _, tc := range []struct {
				name    string
				mode    string
				ids     []int
				present bool
			}{
				{"values", "add", []int{7, 9}, true},
				{"explicit_empty", "", []int{}, true},
				{"explicit_nil", "", nil, true},
				{"absent", "", nil, false},
			} {
				t.Run(tc.name, func(t *testing.T) {
					actual := &input{Mode: tc.mode, OrderIds: tc.ids, Has: &presence{Mode: tc.present, OrderIds: tc.present}}
					if err := injector.Bind(t.Context(), actual, bindly.WithPlan(route.Plan()), bindly.WithSource(actual)); err != nil {
						t.Fatal(err)
					}
					if actual.Mode != tc.mode || !reflect.DeepEqual(actual.OrderIds, tc.ids) || actual.Has == nil || actual.Has.Mode != tc.present || actual.Has.OrderIds != tc.present {
						t.Fatalf("seeded value or presence changed: %+v, has=%+v", actual, actual.Has)
					}
				})
			}
		})
	}
}

func TestDerivedBindingRejectsCrossFieldCycle(t *testing.T) {
	type input struct {
		A string `parameter:"A,kind=param,in=B"`
		B string `parameter:"B,kind=input,in=A"`
	}
	_, err := BuildBindingSpecs(&spec.Component{}, reflect.TypeFor[input](), nil)
	if err == nil || !strings.Contains(err.Error(), "cyclic derived input binding") {
		t.Fatalf("expected cycle error, got %v", err)
	}
}

func TestDerivedBindingOrdersNestedDependencyChain(t *testing.T) {
	type source struct{ Value string }
	type input struct {
		Last   string `parameter:"Last,kind=param,in=First"`
		First  string `parameter:"First,kind=input,in=Source.Value"`
		Source source `parameter:"Source,kind=component,in=GET:/source"`
	}
	bindings, err := BuildBindingSpecs(&spec.Component{}, reflect.TypeFor[input](), nil)
	if err != nil {
		t.Fatal(err)
	}
	first, last := bindingByPath(t, bindings, "First"), bindingByPath(t, bindings, "Last")
	if first.Priority <= locator.PriorityDependent || last.Priority <= first.Priority {
		t.Fatalf("dependent chain is not ordered: %+v", bindings)
	}
}
