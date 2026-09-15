package compiler

import (
	"encoding/json"
	"strings"
	"testing"

	"github.com/viant/datly/spec"
	plan "github.com/viant/datly/transcribe/handler/ast"
)

func TestBuildInputDerivesCanonicalState(t *testing.T) {
	root := &spec.View{Name: "Orders", Source: &spec.ViewSource{Table: "ORDERS", SQL: "SELECT ID, NAME FROM ORDERS WHERE TENANT=7"}, Columns: []*spec.Column{{Name: "ID", Source: "ID", PrimaryKey: true, Type: spec.TypeRef{Name: "int64"}}, {Name: "NAME", Source: "NAME", Type: spec.TypeRef{Name: "string"}}}}
	child := root.Clone()
	child.Name = "Items"
	child.Source.Table = "ITEMS"
	child.Source.SQL = "SELECT ID, ORDER_ID, NAME FROM ITEMS WHERE VISIBLE=1"
	child.Columns = append(child.Columns, &spec.Column{Name: "ORDER_ID", Source: "ORDER_ID", Type: spec.TypeRef{Name: "int64"}})
	aux := root.Clone()
	aux.Name = "Kinds"
	aux.Source.Table = "KINDS"
	aux.Source.SQL = "SELECT ID,NAME FROM KINDS"
	aux.Auxiliary = true
	root.Relations = []*spec.Relation{{Name: "Items", Holder: "Lines", Cardinality: spec.CardinalityMany, View: child, On: []*spec.RelationLink{{ParentColumn: "ID", ChildColumn: "ORDER_ID"}}}, {Name: "Kinds", View: aux, On: []*spec.RelationLink{{ParentColumn: "ID", ChildColumn: "ID"}}}}
	component := &spec.Component{Name: "Orders", RootView: root}
	before, _ := json.Marshal(component)
	got, err := (&Compiler{}).BuildInput(Request{Component: component, Operation: plan.OperationPatch}, "Order")
	if err != nil {
		t.Fatal(err)
	}
	if len(got.Currents) != 3 || len(got.Component.Parameters) != 6 {
		t.Fatalf("derived state: %+v", got)
	}
	if lookup := got.Currents[1].Lookup; lookup == nil || !lookup.ParentOnly || len(lookup.Columns) != 1 || lookup.Columns[0] != "ORDER_ID" {
		t.Fatalf("child parent projection: %+v", lookup)
	}
	childSQL := got.Component.Views[1].Source.SQL
	if !strings.Contains(childSQL, "WHERE VISIBLE=1") || !strings.Contains(childSQL, "$Unsafe.ProjectCurrentItemsParentKeys($CurrentOrders)") {
		t.Fatalf("child scope lost: %s", childSQL)
	}
	for _, parameter := range got.Component.Parameters {
		if parameter.Name == "ItemsKeys" {
			t.Fatal("child identity-only lookup was retained")
		}
	}

	if !strings.Contains(got.Component.Views[0].Source.SQL, "WHERE TENANT=7") {
		t.Fatal("authored read scope lost")
	}
	after, _ := json.Marshal(component)
	if string(before) != string(after) {
		t.Fatal("canonical input mutated")
	}
}

func TestBuildInputPreservesOverridesAndRejectsConflicts(t *testing.T) {
	for _, conflict := range []bool{false, true} {
		t.Run(map[bool]string{false: "authored", true: "conflicting"}[conflict], func(t *testing.T) {
			view := &spec.View{Name: "Orders", Source: &spec.ViewSource{Table: "ORDERS", SQL: "SELECT ID FROM ORDERS"}, Columns: []*spec.Column{{Name: "ID", Source: "ID", PrimaryKey: true, Type: spec.TypeRef{Name: "int64"}}}}
			previous := view.Clone()
			previous.Name = "PriorOrders"
			identity, _ := previous.Identity()
			current := &spec.Parameter{Name: "Before", Source: spec.BindSource{Kind: "view", Name: "PriorOrders"}, TypeExpr: "[]*AuthoredPrior", Cardinality: "Many"}
			required := false
			body := &spec.Parameter{Name: "Payload", Source: spec.BindSource{Kind: "body", Name: "payload"}, TypeExpr: "*AuthoredOrder", Required: &required}
			component := &spec.Component{Name: "Orders", RootView: view, Views: []*spec.View{previous}, Parameters: []*spec.Parameter{body, current}}
			if conflict {
				component.Parameters = append(component.Parameters, &spec.Parameter{Name: "CurrentOrders", Source: spec.BindSource{Kind: "header", Name: "wrong"}})
			}
			got, err := (&Compiler{}).BuildInput(Request{Component: component, Operation: plan.OperationPatch, ViewBindings: map[string]string{current.Identity(): identity}}, "Order")
			if conflict {
				if err == nil {
					t.Fatal("conflict accepted")
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			if got.Input != "Payload" || got.Currents[0].Param != "Before" || got.Component.Parameters[0].TypeExpr != "*AuthoredOrder" || *got.Component.Parameters[0].Required {
				t.Fatalf("authored authority lost: %+v", got)
			}
		})
	}
}

func TestBuildInputHonorsRootCardinality(t *testing.T) {
	component := &spec.Component{Name: "Order", RootView: &spec.View{Name: "Order", Cardinality: spec.CardinalityOne}}
	got, err := (&Compiler{}).BuildInput(Request{Component: component, Operation: plan.OperationPost}, "OrderView")
	if err != nil {
		t.Fatal(err)
	}
	if got.Component.Parameters[0].TypeExpr != "*OrderView" || got.Component.Parameters[0].Cardinality != "One" {
		t.Fatal("authored root cardinality changed")
	}
	if _, err = (&Compiler{}).BuildInput(Request{Component: component, Operation: plan.OperationPost, Input: "Different"}, "OrderView"); err == nil {
		t.Fatal("explicit input conflict ignored")
	}
}
