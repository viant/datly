package compiler

import (
	"encoding/json"
	"strings"
	"testing"

	"github.com/viant/datly/spec"
	plan "github.com/viant/datly/transcribe/handler/ast"
)

func TestBuildInputRegeneratesOnlySelectedInheritedCurrents(t *testing.T) {
	root := &spec.View{Name: "Orders", Source: &spec.ViewSource{Table: "ORDERS", SQL: "SELECT ID FROM ORDERS WHERE TENANT=7"}, Columns: []*spec.Column{{Name: "ID", Source: "ID", PrimaryKey: true, Type: spec.TypeRef{Name: "int64"}}}}
	child := &spec.View{Name: "Items", Source: &spec.ViewSource{Table: "ITEMS", SQL: "SELECT ID,ORDER_ID FROM ITEMS WHERE VISIBLE=1"}, Columns: []*spec.Column{{Name: "ID", Source: "ID", PrimaryKey: true, Type: spec.TypeRef{Name: "int64"}}, {Name: "ORDER_ID", Source: "ORDER_ID", Type: spec.TypeRef{Name: "int64"}}}}
	root.Relations = []*spec.Relation{{Name: "Items", View: child, On: []*spec.RelationLink{{ParentColumn: "ID", ChildColumn: "ORDER_ID"}}}}
	initial, err := (&Compiler{}).BuildInput(Request{Component: &spec.Component{Name: "Orders", RootView: root}, Operation: plan.OperationPatch}, "Order")
	if err != nil {
		t.Fatal(err)
	}
	independent := &spec.View{Name: "Evidence", Source: &spec.ViewSource{Table: "AUDIT", SQL: "SELECT ID FROM AUDIT WHERE TENANT=7"}, Columns: root.Columns}
	independentID, _ := independent.Identity()
	evidence := &spec.Parameter{Name: "Evidence", Source: spec.BindSource{Kind: "view", Name: "Evidence"}}
	initial.Component.Views = append(initial.Component.Views, independent)
	initial.Component.Parameters = append(initial.Component.Parameters, evidence)
	initial.ViewBindings[evidence.Identity()] = independentID
	eligible := map[string]bool{}
	for _, p := range initial.Component.Parameters {
		if p.Source.Kind == "view" {
			eligible[p.Identity()] = true
		}
	}
	before, _ := json.Marshal(initial.Component)
	bindingsBefore, _ := json.Marshal(initial.ViewBindings)
	initial.Component.RootView.Relations[0].View.Columns = append(initial.Component.RootView.Relations[0].View.Columns, &spec.Column{Name: "EVOLVED", Source: "EVOLVED", Type: spec.TypeRef{Name: "string"}})
	before, _ = json.Marshal(initial.Component)
	next, err := (&Compiler{}).BuildInput(Request{Component: initial.Component, ViewBindings: initial.ViewBindings, RegenerateCurrents: eligible, Operation: plan.OperationPatch}, "Order")
	if err != nil {
		t.Fatal(err)
	}
	if len(next.Currents) != 2 || next.Currents[1].Lookup == nil || !next.Currents[1].Lookup.ParentOnly {
		t.Fatalf("missing rebuilt lookup: %+v", next.Currents)
	}
	seenEvidence, seenEvolved := false, false
	for _, v := range next.Component.Views {
		if v.Name == "Evidence" {
			seenEvidence = v.Source.SQL == independent.Source.SQL
		}
		if v.Name == "CurrentItems" {
			for _, c := range v.Columns {
				if c.Name == "EVOLVED" {
					seenEvolved = true
				}
			}
			if !strings.Contains(v.Source.SQL, "WHERE VISIBLE=1") {
				t.Fatal("scope lost")
			}
		}
	}
	if !seenEvidence || !seenEvolved {
		t.Fatalf("retained independent=%v evolved current=%v", seenEvidence, seenEvolved)
	}
	after, _ := json.Marshal(initial.Component)
	if string(before) != string(after) {
		t.Fatal("caller component mutated")
	}
	bindingsAfter, _ := json.Marshal(initial.ViewBindings)
	if string(bindingsBefore) != string(bindingsAfter) {
		t.Fatal("caller bindings mutated")
	}
	// A second input sharing the selected Current view makes replacement unsafe.
	initial.ViewBindings[evidence.Identity()] = initial.ViewBindings["currentitems|view|currentitems"]
	_, err = (&Compiler{}).BuildInput(Request{Component: initial.Component, ViewBindings: initial.ViewBindings, RegenerateCurrents: eligible, Operation: plan.OperationPatch}, "Order")
	if err == nil || !strings.Contains(err.Error(), "shares view") {
		t.Fatalf("shared ownership accepted: %v", err)
	}
}
