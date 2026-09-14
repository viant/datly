package registry

import (
	"reflect"
	"strings"
	"testing"

	"github.com/viant/bindly"
	bindstate "github.com/viant/bindly/state"
	"github.com/viant/datly/spec"
)

func TestInputContractRetainsRouteEffectiveFields(t *testing.T) {
	type nested struct{ Value int }
	type input struct {
		ID     int
		Nested *nested
	}
	injector, err := bindly.NewInjector()
	if err != nil {
		t.Fatal(err)
	}
	getBinding := bindly.BindingSpec{
		Path: "ID", SourceType: reflect.TypeOf(""),
		Location: bindstate.Location{Kind: "query", In: "id"},
	}
	postBinding := bindly.BindingSpec{
		Path: "Nested.Value", Location: bindstate.Location{Kind: "body", In: "value"},
	}
	getPlan, err := injector.CompilePlan(reflect.TypeOf(input{}), getBinding)
	if err != nil {
		t.Fatal(err)
	}
	postPlan, err := injector.CompilePlan(reflect.TypeOf(input{}), postBinding)
	if err != nil {
		t.Fatal(err)
	}
	canonicalPlan, err := injector.CompilePlan(reflect.TypeOf(input{}), getBinding, postBinding)
	if err != nil {
		t.Fatal(err)
	}
	contract, err := NewInputContract(reflect.TypeOf(input{}), testProjection(t, canonicalPlan),
		RouteInput{Route: spec.RouteRef{Method: "GET", Path: "/items"}, Plan: getPlan, Bindings: []bindly.BindingSpec{getBinding}},
		RouteInput{Route: spec.RouteRef{Method: "POST", Path: "/items"}, Plan: postPlan, Bindings: []bindly.BindingSpec{postBinding}},
	)
	if err != nil {
		t.Fatal(err)
	}
	if contract.Type() != reflect.TypeOf(input{}) {
		t.Fatalf("Type() = %v", contract.Type())
	}
	get, ok := contract.ForRoute(spec.RouteRef{Method: "get", Path: "/items"})
	if !ok || get.Type() != reflect.TypeOf(input{}) || get.Plan() != getPlan || len(get.Fields()) != 1 {
		t.Fatalf("GET contract = (%+v, %v)", get, ok)
	}
	field := get.Fields()[0]
	if field.Path() != "ID" || field.SourceType() != reflect.TypeOf("") || field.DestinationType() != reflect.TypeOf(int(0)) {
		t.Fatalf("GET field = %+v", field)
	}
	post, ok := contract.ForRoute(spec.RouteRef{Method: "POST", Path: "/items"})
	if !ok || post.Plan() != postPlan || post.Fields()[0].SourceType() != reflect.TypeOf(int(0)) {
		t.Fatalf("POST contract = (%+v, %v)", post, ok)
	}
}

func TestInputContractRejectsEmptyRouteSet(t *testing.T) {
	type input struct{}
	injector, err := bindly.NewInjector()
	if err != nil {
		t.Fatal(err)
	}
	plan, err := injector.CompilePlan(reflect.TypeOf(input{}))
	if err != nil {
		t.Fatal(err)
	}
	_, err = NewInputContract(reflect.TypeOf(input{}), testProjection(t, plan))
	if err == nil {
		t.Fatal("expected empty route set to fail")
	}
}

func TestInputContractRejectsProjectionForAnotherInputType(t *testing.T) {
	type input struct{ ID int }
	type other struct{ ID int }
	injector, err := bindly.NewInjector()
	if err != nil {
		t.Fatal(err)
	}
	inputPlan, err := injector.CompilePlan(reflect.TypeOf(input{}))
	if err != nil {
		t.Fatal(err)
	}
	otherPlan, err := injector.CompilePlan(reflect.TypeOf(other{}))
	if err != nil {
		t.Fatal(err)
	}
	_, err = NewInputContract(reflect.TypeOf(input{}), testProjection(t, otherPlan), RouteInput{
		Route: spec.RouteRef{Method: "GET", Path: "/items"}, Plan: inputPlan,
	})
	if err == nil || !strings.Contains(err.Error(), "projection targets") {
		t.Fatalf("NewInputContract() error = %v", err)
	}
}

func TestInputContractReturnsDetachedBindingMetadata(t *testing.T) {
	type input struct{ ID int }
	required := true
	binding := bindly.BindingSpec{
		Path: "ID", Location: bindstate.Location{Kind: "query", In: "id"}, Required: &required,
	}
	injector, err := bindly.NewInjector()
	if err != nil {
		t.Fatal(err)
	}
	plan, err := injector.CompilePlan(reflect.TypeOf(input{}), binding)
	if err != nil {
		t.Fatal(err)
	}
	contract, err := NewInputContract(reflect.TypeOf(input{}), testProjection(t, plan), RouteInput{
		Route: spec.RouteRef{Method: "GET", Path: "/items"}, Plan: plan, Bindings: []bindly.BindingSpec{binding},
	})
	if err != nil {
		t.Fatal(err)
	}
	field := contract.routes["GET:/items"].Fields()[0]
	metadata := field.Binding()
	*metadata.Required = false
	if !*contract.routes["GET:/items"].Fields()[0].Binding().Required {
		t.Fatal("mutating returned metadata changed the compiled contract")
	}
}

func TestInputContractRetainsCanonicalAnonymousMetadata(t *testing.T) {
	type taggedInput struct {
		Body struct{} `anonymous:"true"`
	}
	tests := []struct {
		name      string
		typeOf    reflect.Type
		extension any
	}{
		{name: "linked shape", typeOf: reflect.TypeOf(taggedInput{})},
		{name: "generated metadata", typeOf: reflect.TypeOf(struct{ Body struct{} }{}), extension: &spec.Parameter{Tag: `anonymous:"true"`}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			binding := bindly.BindingSpec{Path: "Body", Location: bindstate.Location{Kind: "body"}, Extension: test.extension}
			injector, err := bindly.NewInjector()
			if err != nil {
				t.Fatal(err)
			}
			plan, err := injector.CompilePlan(test.typeOf, binding)
			if err != nil {
				t.Fatal(err)
			}
			contract, err := NewInputContract(test.typeOf, testProjection(t, plan), RouteInput{
				Route: spec.RouteRef{Method: "POST", Path: "/items"}, Plan: plan, Bindings: []bindly.BindingSpec{binding},
			})
			if err != nil {
				t.Fatal(err)
			}
			field := contract.routes["POST:/items"].Fields()[0]
			if !field.Anonymous() {
				t.Fatal("anonymous metadata was not retained")
			}
		})
	}
}

func testProjection(t *testing.T, plan *bindly.Plan) *bindly.Projection {
	t.Helper()
	projection, err := plan.Projection()
	if err != nil {
		t.Fatal(err)
	}
	return projection
}
