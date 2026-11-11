package registry

import (
	"github.com/viant/bindly"
	"github.com/viant/bindly/state"
	"github.com/viant/datly/spec"
	"reflect"
	"testing"
)

type catalogPair struct{ ID *int }
type catalogStrictBody struct{ Body []catalogPair }
type catalogOptBody struct {
	Body []catalogPair `jsonx:"objectToSlice"`
}

func catalogCollectionField(t *testing.T, typ reflect.Type, tag reflect.StructTag, path string) InputField {
	t.Helper()
	injector, err := bindly.NewInjector()
	if err != nil {
		t.Fatal(err)
	}
	binding := bindly.BindingSpec{Path: "Body", Name: "Body", Location: state.Location{Kind: "body"}, BodyInputTag: tag}
	plan, err := injector.CompilePlan(typ, binding)
	if err != nil {
		t.Fatal(err)
	}
	projection, err := plan.Projection()
	if err != nil {
		t.Fatal(err)
	}
	route := spec.RouteRef{Method: "POST", Path: path}
	contract, err := NewInputContract(typ, projection, RouteInput{Route: route, Plan: plan, Bindings: []bindly.BindingSpec{binding}})
	if err != nil {
		t.Fatal(err)
	}
	effective, _ := contract.ForRoute(route)
	return effective.Fields()[0]
}
func TestObjectToSliceCatalogPolicyIdentity(t *testing.T) {
	for _, tc := range []struct {
		name          string
		first, second reflect.Type
		a, b          reflect.StructTag
		conflict      bool
	}{
		{"inferred versus strict", reflect.TypeFor[catalogOptBody](), reflect.TypeFor[catalogStrictBody](), "", "", true},
		{"strict versus inferred", reflect.TypeFor[catalogStrictBody](), reflect.TypeFor[catalogOptBody](), "", "", true},
		{"explicit versus strict", reflect.TypeFor[catalogStrictBody](), reflect.TypeFor[catalogStrictBody](), `jsonx:"objectToSlice"`, "", true},
		{"matching inferred", reflect.TypeFor[catalogOptBody](), reflect.TypeFor[catalogOptBody](), "", "", false},
		{"matching strict", reflect.TypeFor[catalogStrictBody](), reflect.TypeFor[catalogStrictBody](), "", "", false},
		{"equivalent policy order", reflect.TypeFor[catalogStrictBody](), reflect.TypeFor[catalogStrictBody](), `jsonx:"objectToSlice,null=omit"`, `jsonx:" null=omit, objectToSlice "`, false},
		{"null omission conflict", reflect.TypeFor[catalogStrictBody](), reflect.TypeFor[catalogStrictBody](), `jsonx:"objectToSlice,null=omit"`, `jsonx:"objectToSlice"`, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			p := inputProjection{root: spec.RouteRef{Method: "POST", Path: "/parent"}, locations: map[string]int{}}
			a := catalogCollectionField(t, tc.first, tc.a, "/child")
			b := catalogCollectionField(t, tc.second, tc.b, "/parent")
			if err := p.add(a); err != nil {
				t.Fatal(err)
			}
			err := p.add(b)
			if (err != nil) != tc.conflict {
				t.Fatalf("conflict %v got %v", tc.conflict, err)
			}
		})
	}
}
