package generate

import (
	"github.com/viant/datly/spec"
	"testing"
)

func TestRemovedAuxiliaryNullPolicyMetadataRejected(t *testing.T) {
	for _, root := range []bool{true, false} {
		view := &spec.View{Name: "Rows", Cardinality: spec.CardinalityMany, Auxiliary: true, Source: &spec.ViewSource{Table: "records"}}
		component := &spec.Component{RootView: view, Routes: []*spec.Route{{Method: "PATCH"}}}
		target := view
		if !root {
			target = &spec.View{Name: "Children", Cardinality: spec.CardinalityMany, Auxiliary: true, Source: &spec.ViewSource{Table: "children"}}
			view.Relations = []*spec.Relation{{Name: "Children", Holder: "Children", View: target}}
		}
		input := &Input{Component: component}
		if err := input.ValidateLifecycleTarget(true); err != nil {
			t.Fatal("ordinary auxiliary role rejected:", err)
		}
		if root {
			target.RootNullPolicy = "skip-auxiliary"
		} else {
			target.NestedNullPolicy = "skip-auxiliary"
		}
		if err := input.ValidateLifecycleTarget(true); err == nil {
			t.Fatal("removed policy admitted")
		}
	}
}
