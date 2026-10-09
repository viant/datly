package compiler

import (
	"github.com/viant/bindly"
	"github.com/viant/datly/spec"
	"reflect"
	"testing"
)

func TestProjectionPreservesDistinctTransportDestinations(t *testing.T) {
	type input struct {
		FormValue  string
		QueryValue string
	}
	component := &spec.Component{Parameters: []*spec.Parameter{
		{Name: "FormValue", Source: spec.BindSource{Kind: "form", Name: "value"}},
		{Name: "QueryValue", Source: spec.BindSource{Kind: "query", Name: "value"}},
	}}
	typ := reflect.TypeOf(input{})
	bindings, err := BuildBindingSpecs(component, typ, nil)
	if err != nil {
		t.Fatal(err)
	}
	injector, _ := bindly.NewInjector()
	plan, err := injector.CompilePlan(typ, bindings...)
	if err != nil {
		t.Fatal(err)
	}
	projection, err := New(Input{Component: component, InputType: typ}).compileProjection(plan)
	if err != nil {
		t.Fatal(err)
	}
	actual := &input{FormValue: "form", QueryValue: "query"}
	for name, want := range map[string]string{"FormValue": "form", "QueryValue": "query"} {
		value, ok, err := projection.Value(actual, name)
		if err != nil || !ok || value != want {
			t.Fatalf("%s=%v,%v,%v", name, value, ok, err)
		}
	}
	if _, ok, err := projection.Value(actual, "value"); err != nil || ok {
		t.Fatalf("ambiguous alias: %v %v", ok, err)
	}
}
