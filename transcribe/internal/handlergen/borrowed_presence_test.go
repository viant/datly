package handlergen

import (
	"encoding/json"
	"reflect"
	"testing"

	"github.com/viant/datly/spec"
	gen "github.com/viant/datly/transcribe/generate"
	hp "github.com/viant/datly/transcribe/handler/ast"
)

func handlerBorrowedPresenceFixture() (*Generation, *hp.Plan, *gen.Plan) {
	leaf := &spec.View{Name: "Leaves", Auxiliary: true}
	child := &spec.View{Name: "Children", Auxiliary: true, Relations: []*spec.Relation{{Name: "Leaves", Holder: "Leaves", View: leaf}}}
	sibling := &spec.View{Name: "Writable"}
	root := &spec.View{Name: "Rows", Auxiliary: true, Relations: []*spec.Relation{{Name: "Children", Holder: "Children", View: child}, {Name: "OtherChildren", Holder: "OtherChildren", View: child}, {Name: "Writable", Holder: "Writable", View: sibling}}}
	rootID, _ := root.Identity()
	childID, _ := child.Identity()
	leafID, _ := leaf.Identity()
	siblingID, _ := sibling.Identity()
	component := &spec.Component{Name: "Rows", RootView: root}
	input := &gen.Input{Component: component, TargetPackage: "example.com/app", Views: gen.ViewReferences{leafID: &gen.ViewReference{DescriptorKey: "example.com/app.Row", Borrowed: &gen.BorrowedRowAuthority{Declaration: spec.BorrowedSQLRow{BodyPath: "Rows/Children/Leaves"}, BorrowerGraph: []string{rootID, childID, leafID}, Expected: gen.BorrowedLeafContract{Package: "example.com/app", Name: "Row"}}}}}
	planLeaf := &hp.RecordPlan{Identity: leafID, Auxiliary: true}
	planChild := &hp.RecordPlan{Identity: childID, Auxiliary: true, Relations: []*hp.RelationPlan{{FieldPath: hp.FieldPath{"Leaves"}, Child: planLeaf}}}
	semantic := &hp.Plan{Root: &hp.RecordPlan{Identity: rootID, Auxiliary: true, Relations: []*hp.RelationPlan{{FieldPath: hp.FieldPath{"Children"}, Child: planChild}, {FieldPath: hp.FieldPath{"OtherChildren"}, Child: planChild}, {FieldPath: hp.FieldPath{"Writable"}, Child: &hp.RecordPlan{Identity: siblingID}}}}}
	generated := &gen.Plan{Views: []gen.ViewPlan{{Identity: rootID, Name: "Rows", Ownership: gen.ViewGenerated, Fields: []gen.Field{{Name: "Id", Type: "int"}, {Name: "Children", Type: "[]*Children", Tag: `view:"Children"`}}, SetMarkerFields: []string{"Id", "Children"}}, {Identity: childID, Name: "Children", Ownership: gen.ViewGenerated, Fields: []gen.Field{{Name: "Id", Type: "int"}, {Name: "Leaves", Type: "[]*Row", Tag: `view:"Leaves"`}}, SetMarkerFields: []string{"Id", "Leaves"}}}}
	return New(&Context{Component: component}, input, Options{Handler: HandlerOptions{Target: HandlerGo, Go: GoHandlerOptions{Execution: GoExecutionMutation}}}), semantic, generated
}

func TestBorrowedAncestorHandlerPresenceRequiresExactTargetAndEdge(t *testing.T) {
	g, semantic, generated := handlerBorrowedPresenceFixture()
	ancestors, edges, err := g.borrowedAncestorPresence()
	if err != nil {
		t.Fatal(err)
	}
	if len(ancestors) != 2 || len(edges) != 2 {
		t.Fatal("exact path scope missing")
	}
	got := g.setMarkerViews(semantic)
	if !got[semantic.Root.Identity] || !got[semantic.Root.Relations[0].Child.Identity] {
		t.Fatalf("admitted ancestors missing: %v", got)
	}
	refined, err := g.RefinePresence(semantic, generated)
	if err != nil {
		t.Fatal(err)
	}
	if semantic.Root.Entity != nil || semantic.Root.Relations[0].Child.Entity != nil {
		t.Fatal("semantic input mutated")
	}
	var writable bool
	for _, f := range refined.Root.Entity.Fields {
		if f.Name == "Children" {
			writable = f.Writable
		}
	}
	if !writable {
		t.Fatal("exact auxiliary edge setter missing")
	}
	// An alias field with the same child role cannot inherit admitted ownership.
	generated.Views[0].Fields = append(generated.Views[0].Fields, gen.Field{Name: "OtherChildren", Type: "[]*Children", Tag: `view:"Children"`})
	generated.Views[0].SetMarkerFields = append(generated.Views[0].SetMarkerFields, "OtherChildren")
	refined, err = g.RefinePresence(semantic, generated)
	if err != nil {
		t.Fatal(err)
	}
	for _, f := range refined.Root.Entity.Fields {
		if f.Name == "OtherChildren" && f.Writable {
			t.Fatal("alias gained setter eligibility")
		}
	}
	for _, target := range []HandlerTarget{HandlerNone, HandlerVelty, HandlerGo} {
		g.options.Handler.Target = target
		g.options.Handler.Go.Execution = GoExecutionDirect
		a, e, err := g.borrowedAncestorPresence()
		if err != nil || len(a) != 0 || len(e) != 0 {
			t.Fatalf("non-universal target activated %v %v %v", a, e, err)
		}
		selected := g.setMarkerViews(semantic)
		if selected[semantic.Root.Relations[0].Child.Identity] {
			t.Fatal("writable sibling broadened borrowed child markers")
		}
	}
}

func TestBorrowedAncestorHandlerRejectsChangedProof(t *testing.T) {
	g, _, _ := handlerBorrowedPresenceFixture()
	for _, ref := range g.input.Views {
		ref.Borrowed.BorrowerGraph[0] = "view:other"
	}
	if _, _, err := g.borrowedAncestorPresence(); err == nil {
		t.Fatal("stale canonical graph accepted")
	}
}

func TestBorrowedAncestorHandlerDeterministicUnionKeepsOwnership(t *testing.T) {
	g, semantic, _ := handlerBorrowedPresenceFixture()
	before, _ := json.Marshal(g.compiled.Component)
	a, e, err := g.borrowedAncestorPresence()
	if err != nil {
		t.Fatal(err)
	}
	a2, e2, err := g.borrowedAncestorPresence()
	if err != nil || !reflect.DeepEqual(a, a2) || !reflect.DeepEqual(e, e2) {
		t.Fatal("path union unstable")
	}
	after, _ := json.Marshal(g.compiled.Component)
	if string(before) != string(after) || !semantic.Root.Auxiliary {
		t.Fatal("canonical ownership mutated")
	}
}

// Exercise the actual source compiler, borrowed admission, input generation and
// target preparation with a writable sibling selecting the root independently.
func TestBorrowedAncestorPresenceKeepsSelfAndDerivedBranchesExcluded(t *testing.T) {
	for _, kind := range []string{"self", "derived"} {
		t.Run(kind, func(t *testing.T) {
			g, _, _ := handlerBorrowedPresenceFixture()
			if kind == "self" {
				g.compiled.Component.RootView.SelfReference = &spec.SelfReference{Holder: "Descendants"}
			} else {
				g.compiled.Component.RootView.Relations[0].Kind = spec.RelationKindDerived
			}
			a, e, err := g.borrowedAncestorPresence()
			if err != nil || len(a) != 0 || len(e) != 0 {
				t.Fatalf("unsupported path gained ancestor exception %v %v %v", a, e, err)
			}
		})
	}
}
