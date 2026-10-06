package registry

import (
	"context"
	"fmt"
	"github.com/viant/bindly"
	bindstate "github.com/viant/bindly/state"
	docs "github.com/viant/datly/documentation"
	"github.com/viant/datly/spec"
	"reflect"
	"testing"
)

type visibilityChild struct {
	Public     string
	Private    string `internal:"true"`
	BeyondRoot string
}
type visibilityNested struct {
	Value   visibilityChild  `internal:"true"`
	Pointer *visibilityChild `internal:"true"`
}

func TestInputFieldVisibilityUsesRetainedOwner(t *testing.T) {
	cases := []struct {
		name     string
		owner    reflect.Type
		path     string
		internal bool
	}{
		{"public", reflect.TypeFor[visibilityChild](), "Public", false},
		{"private", reflect.TypeFor[visibilityChild](), "Private", true},
		{"beyond root", reflect.TypeFor[visibilityChild](), "BeyondRoot", false},
		{"private value ancestor", reflect.TypeFor[visibilityNested](), "Value.Public", true},
		{"private pointer ancestor", reflect.TypeFor[visibilityNested](), "Pointer.Public", true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			field := InputField{owner: tc.owner, path: tc.path}
			got, err := field.Internal()
			if err != nil {
				t.Fatal(err)
			}
			if got != tc.internal {
				t.Fatalf("visibility=%v want %v", got, tc.internal)
			}
		})
	}
}
func TestInputFieldVisibilityResolutionFailure(t *testing.T) {
	for _, field := range []InputField{{}, {owner: reflect.TypeFor[visibilityChild](), path: "Missing"}} {
		got, err := field.Internal()
		if err == nil || got {
			t.Fatalf("invalid field visibility=%v error=%v", got, err)
		}
	}
}

type visibilityExplicitPublic struct {
	Query string `internal:"false"`
}
type visibilityPrivateAncestor struct {
	Child   visibilityExplicitPublic  `internal:"true"`
	Pointer *visibilityExplicitPublic `internal:"true"`
}

func TestInputFieldPrivateAncestorCannotBeOverridden(t *testing.T) {
	for _, path := range []string{"Child.Query", "Pointer.Query"} {
		field := InputField{owner: reflect.TypeFor[visibilityPrivateAncestor](), path: path}
		got, err := field.Internal()
		if err != nil || !got {
			t.Fatalf("%s visibility=%v error=%v", path, got, err)
		}
	}
}

func TestInputVisibilityCloneRetainsChildOwnerAndBinding(t *testing.T) {
	required := true
	origin := spec.RouteRef{Method: "GET", Path: "/child"}
	field := InputField{owner: reflect.TypeFor[visibilityChild](), path: "BeyondRoot", origin: origin, binding: bindly.BindingSpec{Path: "BeyondRoot", Name: "BeyondRoot", SourceType: reflect.TypeFor[string](), Location: bindstate.Location{Kind: "query", In: "q"}, Required: &required}}
	projection := inputProjection{root: spec.RouteRef{Method: "GET", Path: "/parent"}, locations: map[string]int{}}
	if err := projection.add(field); err != nil {
		t.Fatal(err)
	}
	route := spec.RouteRef{Method: "GET", Path: "/parent"}
	original := &InputContract{typeOf: reflect.TypeFor[visibilityRootFixture](), routes: map[string]*RouteInputContract{route.String(): {route: route, fields: projection.fields}}}
	snapshot, err := (docs.Loader{}).Load(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	clone := original.WithDocumentation(snapshot)
	got := clone.routes[route.String()].fields[0]
	private, err := got.Internal()
	if err != nil || private {
		t.Fatalf("child visibility=%v error=%v", private, err)
	}
	if got.owner != field.owner || got.Path() != field.Path() || got.Origin() != origin || !*got.Binding().Required || got.Documentation() != snapshot {
		t.Fatal("child owner/path/origin/requiredness/documentation changed")
	}
	if original.routes[route.String()].fields[0].Documentation() != nil || original.routes[route.String()].fields[0].owner != field.owner {
		t.Fatal("original metadata mutated")
	}
}

type visibilityRootFixture struct{ Root string }

func TestCatalogCollisionRetainsSelectedOwnerThroughDocumentation(t *testing.T) {
	for _, parentPrivate := range []bool{false, true} {
		t.Run(fmt.Sprint(parentPrivate), func(t *testing.T) {
			parentTag := reflect.StructTag(`internal:"false"`)
			childTag := reflect.StructTag(`internal:"true"`)
			if parentPrivate {
				parentTag, childTag = childTag, parentTag
			}
			parentType := reflect.StructOf([]reflect.StructField{{Name: "Query", Type: reflect.TypeFor[string](), Tag: parentTag}, {Name: "Dependency", Type: reflect.TypeFor[string]()}})
			childType := reflect.StructOf([]reflect.StructField{{Name: "ChildQuery", Type: reflect.TypeFor[string](), Tag: childTag}})
			parentRef := spec.RouteRef{Method: "GET", Path: "/parent"}
			childRef := spec.RouteRef{Method: "GET", Path: "/child"}
			required, optional := true, false
			makeContract := func(typ reflect.Type, ref spec.RouteRef, bindings []bindly.BindingSpec) *InputContract {
				injector, err := bindly.NewInjector()
				if err != nil {
					t.Fatal(err)
				}
				plan, err := injector.CompilePlan(typ, bindings...)
				if err != nil {
					t.Fatal(err)
				}
				contract, err := NewInputContract(typ, testProjection(t, plan), RouteInput{Route: ref, Plan: plan, Bindings: bindings})
				if err != nil {
					t.Fatal(err)
				}
				return contract
			}
			parent := makeContract(parentType, parentRef, []bindly.BindingSpec{{Path: "Dependency", Name: "Dependency", Location: bindstate.Location{Kind: "component", In: childRef.String()}}, {Path: "Query", Name: "Query", Location: bindstate.Location{Kind: "query", In: "q"}, Required: &optional}})
			child := makeContract(childType, childRef, []bindly.BindingSpec{{Path: "ChildQuery", Name: "ChildQuery", Location: bindstate.Location{Kind: "query", In: "q"}, Required: &required}})
			parentKey := spec.Key{Kind: spec.KindComponent, Name: "Parent"}
			childKey := spec.Key{Kind: spec.KindComponent, Name: "Child"}
			catalogFor := func(p, c *InputContract) *InputCatalog {
				result, err := NewInputCatalog([]*RegisteredComponent{{Component: &spec.Component{Key: parentKey, Routes: []*spec.Route{{Method: "GET", Path: "/parent"}}}, Input: p}, {Component: &spec.Component{Key: childKey, Routes: []*spec.Route{{Method: "GET", Path: "/child"}}}, Input: c}})
				if err != nil {
					t.Fatal(err)
				}
				return result
			}
			beforeParent, _ := parent.ForRoute(parentRef)
			beforeChild, _ := child.ForRoute(childRef)
			fields, err := catalogFor(parent, child).FieldsFor(parentKey, parentRef)
			if err != nil {
				t.Fatal(err)
			}
			snapshot, err := (docs.Loader{}).Load(context.Background())
			if err != nil {
				t.Fatal(err)
			}
			clonedParent, clonedChild := parent.WithDocumentation(snapshot), child.WithDocumentation(snapshot)
			clonedFields, err := catalogFor(clonedParent, clonedChild).FieldsFor(parentKey, parentRef)
			if err != nil {
				t.Fatal(err)
			}
			for _, list := range [][]InputField{fields, clonedFields} {
				if len(list) != 1 {
					t.Fatalf("fields=%v", list)
				}
				f := list[0]
				hidden, err := f.Internal()
				if err != nil {
					t.Fatal(err)
				}
				if f.owner != parentType || f.Path() != "Query" || f.Origin() != parentRef || hidden != parentPrivate || f.Binding().Required == nil || !*f.Binding().Required {
					t.Fatal("selected parent ownership or merged requiredness changed")
				}
			}
			afterParent, _ := clonedParent.ForRoute(parentRef)
			afterChild, _ := clonedChild.ForRoute(childRef)
			if afterParent.Plan() != beforeParent.Plan() || afterChild.Plan() != beforeChild.Plan() || clonedParent.projection != parent.projection || clonedChild.projection != child.projection {
				t.Fatal("canonical plans/projections rebuilt")
			}
			if clonedFields[0].Documentation() != snapshot || fields[0].Documentation() != nil || beforeParent.Fields()[1].Documentation() != nil || *beforeParent.Fields()[1].Binding().Required {
				t.Fatal("cloning or requiredness mutated source metadata")
			}
		})
	}
}
