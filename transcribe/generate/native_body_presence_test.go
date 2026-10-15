package generate

import (
	"reflect"
	"strings"
	"testing"

	"github.com/viant/datly/spec"
)

func TestNativeBodyAuxiliaryPresence(t *testing.T) {
	for _, mode := range []string{"body", "unselected", "no-body", "internal", "derived", "linked", "borrowed", "multiple-bodies"} {
		t.Run(mode, func(t *testing.T) {
			component, root, child := setMarkerComponent()
			child.Auxiliary = true
			rootID, _ := root.Identity()
			childID, _ := child.Identity()
			input := Input{Component: component, SetMarkerViews: map[string]bool{rootID: true, childID: true}}
			if err := input.ValidateLifecycleTarget(true); err != nil {
				t.Fatal(err)
			}
			plan, err := New(Input{Component: component}).Plan()
			if err != nil {
				t.Fatal(err)
			}
			switch mode {
			case "unselected":
				input.nativeMutationTargetSelected = false
			case "no-body":
				component.Parameters = nil
			case "internal":
				component.Parameters[0].Source.Kind = "internal"
			case "derived":
				root.Relations[0].Kind = spec.RelationKindDerived
			case "linked":
				generatedViewByIdentity(plan, childID).Ownership = ViewLinked
			case "borrowed":
				// A package reference is never ordinary generated-body authority.
				input.Views = ViewReferences{childID: &ViewReference{DescriptorKey: "example.com/app.Row"}}
			case "multiple-bodies":
				component.Parameters = append(component.Parameters, &spec.Parameter{Name: "Other", Source: spec.BindSource{Kind: "body"}})
			}
			err = (&planResolver{input: input, plan: plan}).applySetMarkerViews()
			if mode == "multiple-bodies" {
				if err == nil || !strings.Contains(err.Error(), "ambiguous") {
					t.Fatalf("error = %v", err)
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			want := []string{"Id", "Name", "Internal"}
			if mode == "body" {
				want = append(want, "Items")
			}
			if got := generatedViewByIdentity(plan, rootID).SetMarkerFields; !reflect.DeepEqual(got, want) {
				t.Fatalf("markers = %v, want %v", got, want)
			}
			if !child.Auxiliary {
				t.Fatal("body presence changed persistence ownership")
			}
		})
	}
}

func TestNativeBodyPresenceDoesNotAdmitIndependentRead(t *testing.T) {
	component, root, child := setMarkerComponent()
	child.Auxiliary = true
	independent := &spec.View{Name: "Current", Auxiliary: true, Relations: []*spec.Relation{{Holder: "Lookup", View: child}}}
	component.Views = []*spec.View{independent}
	component.Parameters = append(component.Parameters, &spec.Parameter{Name: "Current", Source: spec.BindSource{Kind: "view", Name: "Current"}})
	rootID, _ := root.Identity()
	childID, _ := child.Identity()
	independentID, _ := independent.Identity()
	input := Input{Component: component}
	if err := input.ValidateLifecycleTarget(true); err != nil {
		t.Fatal(err)
	}
	plan := &Plan{Views: []ViewPlan{{Identity: rootID, Ownership: ViewGenerated}, {Identity: childID, Ownership: ViewGenerated}, {Identity: independentID, Ownership: ViewGenerated}}}
	edges := map[borrowedPresenceEdge]bool{}
	planned := map[string]*ViewPlan{}
	for i := range plan.Views {
		planned[plan.Views[i].Identity] = &plan.Views[i]
	}
	if err := (&planResolver{input: input, plan: plan}).addNativeBodyPresenceEdges(planned, nil, edges); err != nil {
		t.Fatal(err)
	}
	if len(edges) != 1 || !edges[borrowedPresenceEdge{parent: rootID, holder: "Items", child: childID}] {
		t.Fatalf("edges = %v", edges)
	}
}
