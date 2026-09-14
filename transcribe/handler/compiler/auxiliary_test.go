package compiler

import (
	"testing"

	"github.com/viant/datly/spec"
	plan "github.com/viant/datly/transcribe/handler/ast"
)

func TestAuxiliarySubtreeKeepsBusinessGraphWithoutWriteRequirements(t *testing.T) {
	for _, operation := range []plan.Operation{plan.OperationPost, plan.OperationPut, plan.OperationPatch} {
		t.Run(string(operation), func(t *testing.T) {
			component, item, detail := recursiveComponent()
			item.Auxiliary = true
			detail.Source = &spec.ViewSource{SQL: "SELECT 1 AS ID, 2 AS ITEM_ID"}
			for _, column := range detail.Columns {
				column.PrimaryKey = false
			}
			request := Request{Component: component, Operation: operation}
			if operation == plan.OperationPatch {
				request.Current = "CurrentOrders"
				request.ViewBindings = testViewBindings(t, component, viewBindingIndex{param: 2, view: 0})
			}
			before := len(component.Parameters)
			compiled, err := (&Compiler{}).Compile(request)
			if err != nil {
				t.Fatal(err)
			}
			if len(component.Parameters) != before || len(compiled.Root.Relations) != 1 || len(compiled.Root.Relations[0].Child.Relations) != 1 {
				t.Fatal("auxiliary business input/graph was dropped")
			}
			child := compiled.Root.Relations[0].Child
			descendant := child.Relations[0].Child
			for _, record := range []*plan.RecordPlan{child, descendant} {
				if !record.Auxiliary || record.Sequence != nil || len(record.Write.Allowed) != 0 || record.Write.Missing != "" || record.Write.Existing != "" {
					t.Fatalf("auxiliary mutation plan=%+v", record)
				}
			}
			if len(compiled.Root.Write.Allowed) == 0 {
				t.Fatal("writable root lost mutation")
			}
		})
	}
}

func TestAuxiliaryCurrentIndexRemainsPlanned(t *testing.T) {
	component, item, detail := recursiveComponent()
	item.Auxiliary = true
	itemIdentity, _ := item.Identity()
	detailIdentity, _ := detail.Identity()
	compiled, err := (&Compiler{}).Compile(Request{Component: component, Operation: plan.OperationPatch, Current: "CurrentOrders", ViewBindings: testViewBindings(t, component, viewBindingIndex{param: 2, view: 0}, viewBindingIndex{param: 3, view: 1}, viewBindingIndex{param: 4, view: 2}), Currents: []CurrentBinding{{ViewIdentity: itemIdentity, Param: "CurrentItems"}, {ViewIdentity: detailIdentity, Param: "CurrentDetails"}}})
	if err != nil {
		t.Fatal(err)
	}
	child := compiled.Root.Relations[0].Child
	if child.Current == nil || child.Current.ParamIdentity == "" || child.Relations[0].Child.Current == nil {
		t.Fatal("auxiliary current/index authority was dropped")
	}
}

func TestAuxiliaryRootNeedsNoMutationTableOrKey(t *testing.T) {
	for _, operation := range []plan.Operation{plan.OperationPost, plan.OperationPut, plan.OperationPatch} {
		t.Run(string(operation), func(t *testing.T) {
			component := testComponent()
			component.RootView.Auxiliary = true
			component.RootView.Source = &spec.ViewSource{SQL: "SELECT 1 AS ID"}
			for _, column := range component.RootView.Columns {
				column.PrimaryKey = false
			}
			actual, err := (&Compiler{}).Compile(Request{Component: component, Operation: operation})
			if err != nil {
				t.Fatal(err)
			}
			if !actual.Root.Auxiliary || actual.Root.Current != nil || actual.Root.Sequence != nil || len(actual.Root.Write.Allowed) > 0 {
				t.Fatalf("read-only root=%+v", actual.Root)
			}
		})
	}
}
