package compiler

import (
	"fmt"
	"strings"
	"sync"
	"testing"

	"github.com/viant/datly/spec"
	handlerast "github.com/viant/datly/transcribe/handler/ast"
)

func TestCompilerBuildsRootPatchPlan(t *testing.T) {
	component := testComponent()
	plan, err := (&Compiler{}).Compile(Request{
		Component: component, ViewBindings: testViewBindings(t, component, viewBindingIndex{param: 2, view: 0}),
		Operation: handlerast.OperationPatch, Current: "CurrentEvents",
	})
	if err != nil {
		t.Fatalf("Compile() error = %v", err)
	}
	if plan.Root == nil || plan.Root.Table != "EVENTS" || plan.Root.Cardinality != spec.CardinalityMany {
		t.Fatalf("root = %+v", plan.Root)
	}
	if plan.Root.Sequence == nil || strings.Join(plan.Root.Sequence.Selector, "/") != "Id" || strings.Join(plan.Root.Sequence.Destination, ".") != "Input.Events" {
		t.Fatalf("sequence = %+v", plan.Root.Sequence)
	}
	if plan.Root.Current == nil || plan.Root.Current.InputPath[1] != "CurrentEvents" || plan.Root.Current.Keys[0].Field != "Id" {
		t.Fatalf("current = %+v", plan.Root.Current)
	}
	if plan.Root.Write.Existing != handlerast.ActionUpdate || plan.Root.Write.Missing != handlerast.ActionInsert || len(plan.Root.Write.Allowed) != 2 {
		t.Fatalf("write = %+v", plan.Root.Write)
	}
	if plan.Output == nil || strings.Join(plan.Output.Path, ".") != "Output.Data" {
		t.Fatalf("output = %+v", plan.Output)
	}
}

func TestCompilerCanBeReusedConcurrently(t *testing.T) {
	compiler := &Compiler{}
	component := testComponent()
	const workers = 16
	errors := make(chan error, workers)
	var group sync.WaitGroup
	for index := 0; index < workers; index++ {
		group.Add(1)
		go func() {
			defer group.Done()
			plan, err := compiler.Compile(Request{Component: component, Operation: handlerast.OperationPost})
			if err == nil && (plan == nil || plan.Root == nil || plan.Root.Write.Order != 0) {
				err = fmt.Errorf("unexpected plan: %+v", plan)
			}
			errors <- err
		}()
	}
	group.Wait()
	close(errors)
	for err := range errors {
		if err != nil {
			t.Fatal(err)
		}
	}
}

func TestCompilerRejectsNonPrimaryPatchKey(t *testing.T) {
	_, err := (&Compiler{}).Compile(Request{Component: testComponent(), Operation: handlerast.OperationPatch, Current: "CurrentEvents", Key: "Name"})
	if err == nil || !strings.Contains(err.Error(), "not the canonical primary key") {
		t.Fatalf("Compile() error = %v", err)
	}
}

func TestCompilerPlansIntegerSequences(t *testing.T) {
	for _, typeName := range []string{"int", "int8", "int16", "int32", "int64", "uint", "uint8", "uint16", "uint32", "uint64"} {
		t.Run(typeName, func(t *testing.T) {
			component := testComponent()
			component.RootView.Columns[0].Type.Name = typeName
			plan, err := (&Compiler{}).Compile(Request{Component: component, Operation: handlerast.OperationPost})
			if err != nil || plan.Root.Sequence == nil || strings.Join(plan.Root.Sequence.Selector, "/") != "Id" {
				t.Fatalf("Compile() sequence = %+v, error = %v", plan.Root.Sequence, err)
			}
		})
	}
}

func testComponent() *spec.Component {
	return &spec.Component{
		Name: "Events",
		Parameters: []*spec.Parameter{
			{Name: "Events", Source: spec.BindSource{Kind: "body", Name: "Data"}, TypeExpr: "[]*Event", Cardinality: string(spec.CardinalityMany)},
			{Name: "Data", Source: spec.BindSource{Kind: "output", Name: "body"}, TypeExpr: "[]*Event"},
			{Name: "CurrentEvents", Source: spec.BindSource{Kind: "view", Name: "CurrentEvents"}, TypeExpr: "[]*Event", Cardinality: string(spec.CardinalityMany)},
		},
		RootView: &spec.View{Name: "Events", Source: &spec.ViewSource{Table: "EVENTS"}, Columns: []*spec.Column{
			{Name: "ID", Source: "ID", PrimaryKey: true, Type: spec.TypeRef{Name: "int64"}},
			{Name: "NAME", Source: "NAME", Type: spec.TypeRef{Name: "string"}},
		}},
		Views: []*spec.View{{Name: "CurrentEvents", Columns: []*spec.Column{
			{Name: "ID", Source: "ID", PrimaryKey: true, Type: spec.TypeRef{Name: "int64"}},
			{Name: "NAME", Source: "NAME", Type: spec.TypeRef{Name: "string"}},
		}}},
	}
}

type viewBindingIndex struct {
	param int
	view  int
}

func testViewBindings(t *testing.T, component *spec.Component, bindings ...viewBindingIndex) map[string]string {
	t.Helper()
	result := map[string]string{}
	for _, binding := range bindings {
		identity, err := component.Views[binding.view].Identity()
		if err != nil {
			t.Fatal(err)
		}
		result[component.Parameters[binding.param].Identity()] = identity
	}
	return result
}
