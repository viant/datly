package registry

import (
	"context"
	"reflect"
	"testing"

	"github.com/viant/bindly"
	bindstate "github.com/viant/bindly/state"
	docs "github.com/viant/datly/documentation"
	"github.com/viant/datly/mcp/tool/testdata/customjson"
	"github.com/viant/datly/spec"
)

func TestWireTypeMetadataRetention(t *testing.T) {
	in := reflect.TypeFor[customjson.Input]()
	binding := bindly.BindingSpec{Path: "Body", Name: "Body", SourceType: reflect.TypeFor[*customjson.Request](), Location: bindstate.Location{Kind: "body"}}
	injector, _ := bindly.NewInjector()
	plan, err := injector.CompilePlan(in, binding)
	if err != nil {
		t.Fatal(err)
	}
	ref := spec.RouteRef{Method: "POST", Path: "/requests"}
	carrier := map[string]reflect.Type{"Body": reflect.TypeFor[customjson.RequestWire]()}
	contract, err := NewInputContract(in, testProjection(t, plan), RouteInput{Route: ref, Plan: plan, Bindings: []bindly.BindingSpec{binding}, WireTypes: carrier})
	if err != nil {
		t.Fatal(err)
	}
	carrier["Body"] = reflect.TypeFor[customjson.OtherWire]()
	snapshot, err := (docs.Loader{}).Load(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	route, _ := contract.WithDocumentation(snapshot).ForRoute(ref)
	if route.Plan() != plan {
		t.Fatal("documentation changed plan")
	}
	fields := route.Fields()
	fields[0].wireType = nil
	optional, err := route.WithOptionalBindings("Body")
	if err != nil {
		t.Fatal(err)
	}
	for _, result := range []*RouteInputContract{route, optional} {
		field := result.Fields()[0]
		if field.WireType() != reflect.TypeFor[customjson.RequestWire]() || field.SourceType() != binding.SourceType || field.DestinationType() != binding.SourceType || field.Documentation() != snapshot || field.owner != in || field.Origin() != ref {
			t.Fatalf("metadata changed: %+v", field)
		}
	}
	if optional.Fields()[0].Binding().Required == nil || *optional.Fields()[0].Binding().Required {
		t.Fatal("optional rebuild failed")
	}
}

func TestBorrowedWireTypeOwnerSelection(t *testing.T) {
	type caller struct{ Body *customjson.Request }
	child := InputField{owner: reflect.TypeFor[customjson.Input](), origin: spec.RouteRef{Method: "POST", Path: "/child"}, path: "Body", destinationType: reflect.TypeFor[*customjson.Request](), wireType: reflect.TypeFor[customjson.RequestWire](), binding: bindly.BindingSpec{Path: "Body", SourceType: reflect.TypeFor[*customjson.Request](), Location: bindstate.Location{Kind: "body"}}}
	// A same-named caller field cannot change a borrowed owner's representation.
	parent := child
	parent.owner = reflect.TypeFor[caller]()
	parent.origin = spec.RouteRef{Method: "POST", Path: "/parent"}
	parent.wireType = reflect.TypeFor[customjson.OtherWire]()
	projection := inputProjection{root: parent.origin, locations: map[string]int{}}
	if err := projection.add(child); err != nil {
		t.Fatal(err)
	}
	if err := projection.add(parent); err == nil {
		t.Fatal("conflicting caller authority silently replaced borrowed owner")
	}
	got := projection.fields[0]
	field, err := got.StructField()
	if err != nil || got.owner != child.owner || got.Origin() != child.origin || got.WireType() != child.WireType() || field.Tag.Get("wireType") == "" {
		t.Fatalf("borrowed owner changed: %+v %v", got, err)
	}
	// Rebuilding an optional projected field keeps the selected owner's metadata.
	injector, err := bindly.NewInjector()
	if err != nil {
		t.Fatal(err)
	}
	plan, err := injector.CompilePlan(parent.owner, child.binding)
	if err != nil {
		t.Fatal(err)
	}
	contract, err := NewInputContract(parent.owner, testProjection(t, plan), RouteInput{Route: parent.origin, Plan: plan, Bindings: []bindly.BindingSpec{child.binding}})
	if err != nil {
		t.Fatal(err)
	}
	route, _ := contract.ForRoute(parent.origin)
	route.fields = []InputField{got}
	optional, err := route.WithOptionalBindings("Body")
	if err != nil {
		t.Fatal(err)
	}
	rebuilt := optional.Fields()[0]
	if rebuilt.owner != child.owner || rebuilt.Origin() != child.origin || rebuilt.WireType() != child.WireType() || rebuilt.SourceType() != child.SourceType() || rebuilt.DestinationType() != child.DestinationType() {
		t.Fatalf("borrowed optional metadata changed: %+v", rebuilt)
	}
}
