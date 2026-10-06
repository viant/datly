package compiler

import (
	"github.com/viant/datly/spec"
	"testing"
)

func TestAuxiliaryNullPoliciesDoNotReachDerivedCurrent(t *testing.T) {
	for _, nested := range []bool{false, true} {
		view := &spec.View{Key: spec.Key{Kind: spec.KindView, Name: "Rows"}, Name: "Rows", Auxiliary: true, Source: &spec.ViewSource{Table: "records"}, Columns: []*spec.Column{{Name: "id", Source: "id", PrimaryKey: true}}}
		if nested {
			view.NestedNullPolicy = "skip-auxiliary"
		} else {
			view.RootNullPolicy = "skip-auxiliary"
		}
		component := &spec.Component{}
		generation := &inputGeneration{request: Request{Component: component, ViewBindings: map[string]string{}}, names: map[string]*spec.Parameter{}}
		if err := generation.appendCurrent(view, "CurrentRows", "r.id IN (1)"); err != nil {
			t.Fatal(err)
		}
		if len(component.Views) != 1 {
			t.Fatal("missing real derived Current")
		}
		current := component.Views[0]
		if current.RootNullPolicy != "" || current.NestedNullPolicy != "" || len(current.Relations) != 0 {
			t.Fatalf("mutation policy reached Current: %+v", current)
		}
		if nested && view.NestedNullPolicy != "skip-auxiliary" || !nested && view.RootNullPolicy != "skip-auxiliary" {
			t.Fatal("source policy changed")
		}
	}
}
