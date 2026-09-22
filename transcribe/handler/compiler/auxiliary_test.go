package compiler

import (
	"testing"

	"github.com/viant/datly/spec"
	plan "github.com/viant/datly/transcribe/handler/ast"
)

func TestAuxiliaryViewDoesNotImplicitlyMakeDescendantsAuxiliary(t *testing.T) {
	component, item, _ := recursiveComponent()
	item.Auxiliary = true
	compiled, err := (&Compiler{}).Compile(Request{Component: component, Operation: plan.OperationPost})
	if err != nil {
		t.Fatal(err)
	}
	child := compiled.Root.Relations[0].Child
	descendant := child.Relations[0].Child
	if !child.Auxiliary || len(child.Write.Allowed) != 0 {
		t.Fatalf("authored auxiliary child=%+v", child)
	}
	if descendant.Auxiliary || descendant.Write.Missing != plan.ActionInsert {
		t.Fatalf("writable descendant=%+v", descendant)
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
