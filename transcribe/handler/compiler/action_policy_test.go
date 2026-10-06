package compiler

import (
	"testing"

	"github.com/viant/datly/spec"
	plan "github.com/viant/datly/transcribe/handler/ast"
)

func TestCompilerInsertDeletePolicyUsesRealCurrent(t *testing.T) {
	component := testComponent()
	component.RootView.WriterActionPolicy = "insert-delete"
	component.RootView.Columns = append(component.RootView.Columns, &spec.Column{Name: "REMOVE", DeleteMarker: true, Type: spec.TypeRef{Name: "bool"}})
	result, err := (&Compiler{}).Compile(Request{Component: component, Operation: plan.OperationPatch, Current: "CurrentEvents", ViewBindings: testViewBindings(t, component, viewBindingIndex{param: 2, view: 0})})
	if err != nil {
		t.Fatal(err)
	}
	if result.Root.Current == nil || result.Root.Write.ActionPolicy != "insert-delete" || result.Root.Write.Existing != plan.ActionInsert || result.Root.Write.Missing != plan.ActionInsert || len(result.Root.Write.Allowed) != 2 || result.Root.Write.DeleteMarker.Field != "Remove" {
		t.Fatalf("plan=%+v", result.Root)
	}
}

func TestCompilerInsertDeleteAdmission(t *testing.T) {
	for _, test := range []struct {
		name      string
		mutate    func(*spec.View, *plan.RecordPlan)
		operation plan.Operation
	}{
		{"unknown", func(v *spec.View, r *plan.RecordPlan) { v.WriterActionPolicy = "unknown" }, plan.OperationPatch},
		{"post", func(v *spec.View, r *plan.RecordPlan) {}, plan.OperationPost},
		{"put", func(v *spec.View, r *plan.RecordPlan) {}, plan.OperationPut},
		{"auxiliary", func(v *spec.View, r *plan.RecordPlan) { r.Auxiliary = true }, plan.OperationPatch},
		{"missingCurrent", func(v *spec.View, r *plan.RecordPlan) { r.Current = nil }, plan.OperationPatch},
		{"missingTable", func(v *spec.View, r *plan.RecordPlan) { r.Table = "" }, plan.OperationPatch},
		{"missingKeys", func(v *spec.View, r *plan.RecordPlan) { r.Keys = nil }, plan.OperationPatch},
		{"missingMarker", func(v *spec.View, r *plan.RecordPlan) { r.Write.DeleteMarker = plan.FieldRef{} }, plan.OperationPatch},
		{"descendants", func(v *spec.View, r *plan.RecordPlan) { v.Relations = []*spec.Relation{{Name: "Child"}} }, plan.OperationPatch},
		{"identityOverride", func(v *spec.View, r *plan.RecordPlan) { v.WriterIdentityPolicy = "assigned-update" }, plan.OperationPatch},
		{"predicate", func(v *spec.View, r *plan.RecordPlan) { g := 1; v.MutationPredicateGroup = &g }, plan.OperationPatch},
		{"token", func(v *spec.View, r *plan.RecordPlan) { r.Write.ConcurrencyToken = plan.FieldRef{Field: "Version"} }, plan.OperationPatch},
	} {
		t.Run(test.name, func(t *testing.T) {
			v := &spec.View{WriterActionPolicy: "insert-delete"}
			r := &plan.RecordPlan{Table: "items", Current: &plan.CurrentPlan{}, Keys: []plan.KeyPart{{Field: "ID"}}, Write: plan.WritePolicy{DeleteMarker: plan.FieldRef{Field: "Remove"}}}
			test.mutate(v, r)
			if err := compileWriterActionPolicy(r, v, test.operation); err == nil {
				t.Fatal("unsupported policy admitted")
			}
		})
	}
}
