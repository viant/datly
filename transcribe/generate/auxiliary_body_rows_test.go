package generate

import (
	"strings"
	"testing"

	"github.com/viant/datly/spec"
)

func TestAuxiliaryBodyGraphConcreteAdmission(t *testing.T) {
	for _, mode := range []string{"all auxiliary", "pending discovery", "pending final", "physical child", "derived", "self", "cycle"} {
		t.Run(mode, func(t *testing.T) {
			_, root, child := setMarkerComponent()
			for _, view := range []*spec.View{root, child} {
				view.Auxiliary = true
				view.Source = &spec.ViewSource{Table: "records"}
			}
			r := &planResolver{requireConcreteHelpers: true}
			switch mode {
			case "pending discovery", "pending final":
				child.Auxiliary = false
				child.Source = &spec.ViewSource{URI: "sql/child.sql"}
				r.requireConcreteHelpers = mode == "pending final"
			case "physical child":
				child.Auxiliary = false
			case "derived":
				root.Relations[0].Kind = spec.RelationKindDerived
			case "self":
				child.SelfReference = &spec.SelfReference{}
			case "cycle":
				child.Relations = []*spec.Relation{{Holder: "Parents", View: root}}
			}
			graph := r.auxiliaryBodyGraph(root)
			want := mode == "all auxiliary" || mode == "pending discovery"
			if (len(graph) == 2) != want {
				t.Fatalf("graph=%v mode=%s", graph, mode)
			}
		})
	}
}

func TestAuxiliaryBodyGraphSharingAndCollisions(t *testing.T) {
	for _, mode := range []string{"read", "competing body", "canonical type", "cast", "destination", "mapping", "read relation"} {
		t.Run(mode, func(t *testing.T) {
			p, owner, current := ordinaryBodyPlanner()
			owner.Auxiliary = true
			p.auxiliaryGraph = map[*spec.View]bool{owner: true}
			p.owners = map[string]*spec.View{"Record": owner}
			p.explicitReads = map[*spec.View]bool{current: true}
			p.ordinaryReads = map[*spec.View]bool{current: true}
			p.sharedBodyRows = map[*spec.View]*spec.View{}
			dest := "record.go"
			switch mode {
			case "competing body":
				p.auxiliaryGraph[current] = true
			case "canonical type":
				p.readTypes[current] = []string{"Other"}
			case "cast":
				current.Columns[0].Type.Name = "string"
			case "destination":
				dest = "other.go"
			case "mapping":
				owner.Columns = append(owner.Columns, &spec.Column{Name: "OtherId", Source: "id", Type: spec.TypeRef{Name: "int"}})
			case "read relation":
				current.Relations = []*spec.Relation{{Holder: "Children", View: &spec.View{Name: "Children"}}}
			}
			err := p.assign(current, "Record", dest)
			if mode == "read" {
				if err != nil || p.sharedBodyRows[current] != owner {
					t.Fatalf("share: %v", err)
				}
				return
			}
			if err == nil {
				t.Fatal("invalid graph sharing admitted")
			}
			if mode == "competing body" && !strings.Contains(err.Error(), "compete") {
				t.Fatal(err)
			}
		})
	}
}

func TestAuxiliaryBodyLinkedHolderPresence(t *testing.T) {
	component, root, child := setMarkerComponent()
	for _, view := range []*spec.View{root, child} {
		view.Auxiliary = true
		view.Source = &spec.ViewSource{Table: "records"}
	}
	root.EntityHooks = "BatchLifecycle"
	rootID, _ := root.Identity()
	childID, _ := child.Identity()
	plan := &Plan{Package: "example.com/rows", RootViewType: "OrdersView", Views: []ViewPlan{{Identity: rootID, Name: "OrdersView", Ownership: ViewGenerated}, {Identity: childID, Name: "ItemsView", Ownership: ViewLinked}}}
	input := Input{Component: component, nativeMutationTargetSelected: true}
	r := &planResolver{input: input, plan: plan, requireConcreteHelpers: true}
	edges := map[borrowedPresenceEdge]bool{}
	planned := map[string]*ViewPlan{rootID: &plan.Views[0], childID: &plan.Views[1]}
	if err := r.addNativeBodyPresenceEdges(planned, map[string]bool{childID: true}, edges); err != nil {
		t.Fatal(err)
	}
	if !edges[borrowedPresenceEdge{rootID, "Items", childID}] {
		t.Fatalf("linked holder presence lost: %v", edges)
	}
	if len(plan.Views[1].Fields) != 0 || len(plan.Views[1].SetMarkerFields) != 0 {
		t.Fatal("linked package marker mutated")
	}
}
