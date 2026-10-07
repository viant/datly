package generate

import (
	"github.com/viant/datly/spec"
	"reflect"
	"strings"
	"sync"
	"testing"
)

func borrowedAncestorFixture(t *testing.T) (Input, *Plan, string, string) {
	t.Helper()
	c, root, child := setMarkerComponent()
	root.Auxiliary = true
	child.Auxiliary = true
	parentID, _ := root.Identity()
	childID, _ := child.Identity()
	root.Relations = append(root.Relations, &spec.Relation{Name: "Alias", Holder: "Alias", View: child, Cardinality: spec.CardinalityMany})
	in := Input{Component: c, TargetPackage: "example.com/app", Views: ViewReferences{childID: &ViewReference{DescriptorKey: "example.com/app.Row", Borrowed: &BorrowedRowAuthority{Declaration: spec.BorrowedSQLRow{BodyPath: "Orders/Items"}, BorrowerGraph: []string{parentID, childID}, Expected: BorrowedLeafContract{Package: "example.com/app", Name: "Row"}}}}, SetMarkerViews: map[string]bool{parentID: true, childID: true}}
	plan := &Plan{Views: []ViewPlan{{Identity: parentID, Name: "OrdersView", Ownership: ViewGenerated, Fields: []Field{{Name: "Id"}, {Name: "Name"}, {Name: "Internal"}, {Name: "Items"}, {Name: "Alias"}}}, {Identity: childID, Name: "Row", Ownership: ViewLinked, SetMarkerFields: []string{"Id", "OrderId"}}}}
	return in, plan, parentID, childID
}

func TestBorrowedAncestorMarkersRequireExplicitNativeTargetAndExactHolder(t *testing.T) {
	for _, selected := range []bool{false, true} {
		t.Run(map[bool]string{false: "unselected", true: "selected"}[selected], func(t *testing.T) {
			input, plan, parent, child := borrowedAncestorFixture(t)
			if selected {
				if err := input.ValidateLifecycleTarget(true); err != nil {
					t.Fatal(err)
				}
			}
			// Mutation setting/asset and marker maps are deliberately insufficient.
			input.Component.Settings = &spec.Settings{Mutation: "patch"}
			input.MutationHandler = &MutationHandlerAsset{}
			r := &planResolver{input: input, plan: plan}
			if err := r.applySetMarkerViews(); err != nil {
				t.Fatal(err)
			}
			want := []string{"Id", "Name", "Internal"}
			if selected {
				want = append(want, "Items")
			}
			if got := generatedViewByIdentity(plan, parent).SetMarkerFields; !reflect.DeepEqual(got, want) {
				t.Fatalf("markers=%v want=%v", got, want)
			}
			if got := generatedViewByIdentity(plan, child); got.Ownership != ViewLinked || !reflect.DeepEqual(got.SetMarkerFields, []string{"Id", "OrderId"}) {
				t.Fatalf("linked leaf changed: %+v", got)
			}
			if !input.Component.RootView.Auxiliary || !input.Component.RootView.Relations[0].View.Auxiliary {
				t.Fatal("persistence ownership changed")
			}
		})
	}
}

func TestBorrowedAncestorMarkersFailClosedOnStaleOrAmbiguousPath(t *testing.T) {
	cases := map[string]func(*Input){
		"wrong-root": func(in *Input) {
			for _, r := range in.Views {
				r.Borrowed.Declaration.BodyPath = "Other/Items"
			}
		},
		"case-mismatch": func(in *Input) {
			for _, r := range in.Views {
				r.Borrowed.Declaration.BodyPath = "Orders/items"
			}
		},
		"stale-graph": func(in *Input) {
			for _, r := range in.Views {
				r.Borrowed.BorrowerGraph[0] = "view:stale"
			}
		},
		"changed-child": func(in *Input) { in.Component.RootView.Relations[0].View.Name = "Changed" },
		"ambiguous-holder": func(in *Input) {
			root := in.Component.RootView
			root.Relations = append(root.Relations, &spec.Relation{Holder: "Items", View: root.Relations[0].View})
		},
		"incomplete-edge": func(in *Input) { in.Component.RootView.Relations = append(in.Component.RootView.Relations, nil) },
		"non-leaf": func(in *Input) {
			child := in.Component.RootView.Relations[0].View
			child.Relations = append(child.Relations, &spec.Relation{Holder: "Nested", View: &spec.View{Name: "Nested"}})
		},
		"multiple-bodies": func(in *Input) {
			in.Component.Parameters = append(in.Component.Parameters, &spec.Parameter{Name: "Other", Source: spec.BindSource{Kind: "body"}})
		},
		"wrong-descriptor": func(in *Input) {
			for _, r := range in.Views {
				r.DescriptorKey = "other.Row"
			}
		},
	}
	for name, mutate := range cases {
		t.Run(name, func(t *testing.T) {
			input, plan, _, _ := borrowedAncestorFixture(t)
			if err := input.ValidateLifecycleTarget(true); err != nil {
				t.Fatal(err)
			}
			mutate(&input)
			if err := (&planResolver{input: input, plan: plan}).applySetMarkerViews(); err == nil {
				t.Fatal("invalid path activated markers")
			}
		})
	}
}

func TestBorrowedAncestorMarkersKeepDerivedExclusion(t *testing.T) {
	input, plan, parent, _ := borrowedAncestorFixture(t)
	input.Component.RootView.Relations[0].Kind = spec.RelationKindDerived
	if err := input.ValidateLifecycleTarget(true); err != nil {
		t.Fatal(err)
	}
	if err := (&planResolver{input: input, plan: plan}).applySetMarkerViews(); err != nil {
		t.Fatal(err)
	}
	if got := generatedViewByIdentity(plan, parent).SetMarkerFields; !reflect.DeepEqual(got, []string{"Id", "Name", "Internal"}) {
		t.Fatalf("derived path markers=%v", got)
	}
}

func TestLifecycleTargetSelectionClearsFailuresAndCopiesIndependently(t *testing.T) {
	var nilInput *Input
	if err := nilInput.ValidateLifecycleTarget(true); err != nil {
		t.Fatal(err)
	}
	input, _, _, _ := borrowedAncestorFixture(t)
	if err := input.ValidateLifecycleTarget(true); err != nil || !input.nativeMutationTargetSelected {
		t.Fatalf("selection err=%v", err)
	}
	copy := New(input)
	if !copy.input.nativeMutationTargetSelected {
		t.Fatal("New erased explicit selection")
	}
	if err := input.ValidateLifecycleTarget(false); err != nil || input.nativeMutationTargetSelected {
		t.Fatal("false did not clear selection")
	}
	if !copy.input.nativeMutationTargetSelected {
		t.Fatal("independent input changed")
	}
	input.Component.RootView.QueueContract = "source-row"
	input.nativeMutationTargetSelected = true
	if err := input.ValidateLifecycleTarget(true); err == nil || !strings.Contains(err.Error(), "queue_contract") || input.nativeMutationTargetSelected {
		t.Fatalf("failed validation retained selection: %v", err)
	}
	var group sync.WaitGroup
	for i := 0; i < 20; i++ {
		group.Add(1)
		go func(selected bool) {
			defer group.Done()
			value := Input{}
			if err := value.ValidateLifecycleTarget(selected); err != nil || value.nativeMutationTargetSelected != selected {
				t.Error("parallel input context leaked")
			}
		}(i%2 == 0)
	}
	group.Wait()
}

func TestLifecycleInternalValidationCannotManufactureOrEraseSelection(t *testing.T) {
	for _, selected := range []bool{false, true} {
		input, _, _, _ := borrowedAncestorFixture(t)
		input.Component.Settings = &spec.Settings{Mutation: "patch"}
		if selected {
			if err := input.ValidateLifecycleTarget(true); err != nil {
				t.Fatal(err)
			}
		}
		if err := input.validateLifecycleTarget(true, false); err != nil {
			t.Fatal(err)
		}
		if err := input.validateLifecycleTarget(false, false); err != nil {
			t.Fatal(err)
		}
		if input.nativeMutationTargetSelected != selected {
			t.Fatal("pure internal validation changed caller selection")
		}
	}
}

func TestBorrowedAncestorPresenceKeepsSelfPathOutsideException(t *testing.T) {
	input, _, _, _ := borrowedAncestorFixture(t)
	input.Component.RootView.SelfReference = &spec.SelfReference{Holder: "Descendants"}
	if err := input.ValidateLifecycleTarget(true); err != nil {
		t.Fatal(err)
	}
	edges, err := (&planResolver{input: input}).borrowedAncestorPresenceEdges()
	if err != nil || len(edges) != 0 {
		t.Fatalf("self path gained relation exception %v %v", edges, err)
	}
}
