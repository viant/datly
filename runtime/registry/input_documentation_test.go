package registry

import (
	"context"
	"github.com/viant/bindly"
	bindstate "github.com/viant/bindly/state"
	docs "github.com/viant/datly/documentation"
	"github.com/viant/datly/spec"
	"reflect"
	"testing"
)

func TestInputDocumentationCloneRetainsCanonicalPlans(t *testing.T) {
	type Input struct{ Query string }
	injector, err := bindly.NewInjector()
	if err != nil {
		t.Fatal(err)
	}
	binding := bindly.BindingSpec{Path: "Query", Name: "Query", Location: bindstate.Location{Kind: "query", In: "q"}}
	plan, err := injector.CompilePlan(reflect.TypeFor[Input](), binding)
	if err != nil {
		t.Fatal(err)
	}
	ref := spec.RouteRef{Method: "GET", Path: "/items"}
	input, err := NewInputContract(reflect.TypeFor[Input](), testProjection(t, plan), RouteInput{Route: ref, Plan: plan, Bindings: []bindly.BindingSpec{binding}})
	if err != nil {
		t.Fatal(err)
	}
	snapshot, err := (docs.Loader{}).Load(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	copy := input.WithDocumentation(snapshot)
	route, _ := copy.ForRoute(ref)
	original, _ := input.ForRoute(ref)
	if copy == input || copy.projection != input.projection || route.Plan() != original.Plan() {
		t.Fatal("annotation attachment rebuilt/mutated canonical binding")
	}
	if original.Fields()[0].Documentation() != nil || route.Fields()[0].Documentation() != snapshot {
		t.Fatal("annotation ownership leaked")
	}
	field := route.Fields()[0]
	source, err := field.StructField()
	if err != nil || source.Name != "Query" || field.Origin() != ref {
		t.Fatalf("canonical field provenance %v %v", source, err)
	}
}
func TestParentInputAnnotationWinsIndependentOfMethodCase(t *testing.T) {
	required := true
	child := InputField{path: "ChildQuery", origin: spec.RouteRef{Method: "GET", Path: "/child"}, binding: bindly.BindingSpec{Name: "ChildQuery", SourceType: reflect.TypeFor[string](), Location: bindstate.Location{Kind: "query", In: "q"}, Required: &required, Extension: &spec.Parameter{Description: "Private authored"}}}
	parent := InputField{path: "Query", origin: spec.RouteRef{Method: "get", Path: "/parent"}, binding: bindly.BindingSpec{Name: "Query", SourceType: reflect.TypeFor[string](), Location: bindstate.Location{Kind: "query", In: "q"}, Extension: &spec.Parameter{Description: "Parent authored"}}}
	projection := inputProjection{root: spec.RouteRef{Method: "GET", Path: "/parent"}, locations: map[string]int{}}
	if err := projection.add(child); err != nil {
		t.Fatal(err)
	}
	if err := projection.add(parent); err != nil {
		t.Fatal(err)
	}
	got := projection.fields[0]
	if got.Path() != "Query" || got.Binding().Extension.(*spec.Parameter).Description != "Parent authored" || got.Binding().Required == nil || !*got.Binding().Required {
		t.Fatal("parent annotations or child requiredness lost")
	}
	if parent.Binding().Required != nil {
		t.Fatal("parent binding was mutated")
	}
}
