package generate

import (
	"fmt"
	"github.com/viant/datly/spec"
	"github.com/viant/datly/tag"
	"reflect"
	"testing"
)

func TestAuxiliaryNullLifecycleRestrictions(t *testing.T) {
	for _, rootPolicy := range []bool{true, false} {
		for _, aux := range []bool{true, false} {
			for _, one := range []bool{true, false} {
				for _, mutation := range []bool{true, false} {
					target := &spec.View{Name: "Rows", Auxiliary: aux}
					root := target
					if rootPolicy {
						target.RootNullPolicy = "skip-auxiliary"
					} else {
						target.Name = "Children"
						target.NestedNullPolicy = "skip-auxiliary"
						root = &spec.View{Name: "Rows", Relations: []*spec.Relation{{View: target}}}
					}
					if one {
						target.Cardinality = spec.CardinalityOne
					}
					method := "PATCH"
					if !mutation {
						method = "GET"
					}
					in := &Input{Component: &spec.Component{RootView: root, Routes: []*spec.Route{{Method: method}}}}
					err := in.ValidateLifecycleTarget(mutation)
					want := aux && !one && mutation
					if (err == nil) != want {
						t.Fatalf("root=%v aux=%v one=%v mutation=%v: %v", rootPolicy, aux, one, mutation, err)
					}
				}
			}
		}
	}
}

func TestAuxiliaryNullGeneratedTagRoundTrip(t *testing.T) {
	for _, root := range []bool{true, false} {
		v := &spec.View{Name: "Rows", Auxiliary: true, Source: &spec.ViewSource{Table: "records"}}
		if root {
			v.RootNullPolicy = "skip-auxiliary"
		} else {
			v.NestedNullPolicy = "skip-auxiliary"
		}
		got, err := appendViewTags(`json:"rows"`, v)
		if err != nil {
			t.Fatal(err)
		}
		decoded, err := tag.ParseView(reflect.StructTag(got).Get("view"))
		if err != nil {
			t.Fatal(err)
		}
		if !decoded.Auxiliary || decoded.Table != "records" || decoded.RootNullPolicy != v.RootNullPolicy || decoded.NestedNullPolicy != v.NestedNullPolicy {
			t.Fatalf("metadata lost: %+v", decoded)
		}
		if reflect.StructTag(got).Get("json") != "rows" {
			t.Fatal("unrelated tag changed")
		}
	}
}

func TestAuxiliaryNullPendingDiscoveryNeverAuthorizesEmission(t *testing.T) {
	for _, root := range []bool{true, false} {
		target := &spec.View{Name: "Rows", Source: &spec.ViewSource{Embeds: []*spec.EmbeddedSQLRef{{Path: "sql/records.sql", Raw: "${embed:sql/records.sql}"}}, SQL: "SELECT * FROM (${embed:sql/records.sql}) r"}}
		parent := target
		if root {
			target.RootNullPolicy = "skip-auxiliary"
		} else {
			target.NestedNullPolicy = "skip-auxiliary"
			parent = &spec.View{Name: "Root", Source: &spec.ViewSource{Table: "parents"}, Relations: []*spec.Relation{{View: target}}}
		}
		input := Input{Component: &spec.Component{RootView: parent, Settings: &spec.Settings{Mutation: "patch"}, Routes: []*spec.Route{{Method: "PATCH"}}}}
		if err := input.validateLifecycleTarget(true, true); err != nil {
			t.Fatal("provisional discovery shape rejected", err)
		}
		if err := input.ValidateLifecycleTarget(true); err == nil {
			t.Fatal("unresolved source authorized dispatch")
		}
		if _, err := New(input).Plan(); err == nil {
			t.Fatal("unresolved source authorized final plan")
		}
		if _, err := New(input).Generate(t.TempDir()); err == nil {
			t.Fatal("unresolved source authorized emission")
		}
		target.Source.Table = "records"
		if err := input.validateLifecycleTarget(true, true); err == nil {
			t.Fatal("known writable source accepted as provisional")
		}
		target.Auxiliary = true
		if err := input.ValidateLifecycleTarget(true); err != nil {
			t.Fatal("resolved auxiliary rejected", err)
		}
		target.Cardinality = spec.CardinalityOne
		if err := input.validateLifecycleTarget(true, true); err == nil {
			t.Fatal("to-one exception introduced")
		}
	}
}

func TestAuxiliaryNullRejectsIndependentCurrentAndDerivedViews(t *testing.T) {
	for _, name := range []string{"Independent", "CurrentRows", "DerivedRows"} {
		for _, pending := range []bool{false, true} {
			t.Run(name+fmt.Sprint(pending), func(t *testing.T) {
				root := &spec.View{Name: "Rows", Source: &spec.ViewSource{Table: "records"}}
				target := &spec.View{Name: name, Auxiliary: true, NestedNullPolicy: "skip-auxiliary", Source: &spec.ViewSource{Table: "children"}}
				c := &spec.Component{RootView: root, Views: []*spec.View{target}, Routes: []*spec.Route{{Method: "PATCH"}}}
				if name == "DerivedRows" {
					root.Relations = []*spec.Relation{{Kind: spec.RelationKindDerived, View: target}}
				}
				in := Input{Component: c}
				if err := in.validateLifecycleTarget(true, pending); err == nil {
					t.Fatal("non-mutation view admitted")
				}
				root.Relations = []*spec.Relation{{View: target}}
				if err := in.validateLifecycleTarget(true, pending); err != nil {
					t.Fatal("genuine body relation rejected", err)
				}
			})
		}
	}
}
