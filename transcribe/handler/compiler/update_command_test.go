package compiler

import (
	"encoding/json"
	"testing"

	"github.com/viant/datly/spec"
	plan "github.com/viant/datly/transcribe/handler/ast"
)

func TestCompilerUpdateCommandDoesNotDeriveCurrentOrSequence(t *testing.T) {
	view := &spec.View{Name: "Records", WriterActionPolicy: "update", Source: &spec.ViewSource{Table: "records", SQL: "SELECT * FROM records"}, Columns: []*spec.Column{{Name: "id", Source: "id", PrimaryKey: true, AutoIncrement: true, Type: spec.TypeRef{Name: "int64"}}, {Name: "name", Source: "name", Type: spec.TypeRef{Name: "string"}}}}
	component := &spec.Component{Name: "Records", RootView: view}
	before, _ := json.Marshal(component)
	request, err := (&Compiler{}).BuildInput(Request{Component: component, Operation: plan.OperationPatch}, "Record")
	if err != nil {
		t.Fatal(err)
	}
	if request.Current != "" || len(request.Currents) != 0 || len(request.Component.Views) != 0 || len(request.Component.Parameters) != 2 {
		t.Fatalf("command inferred read state: %+v", request)
	}
	result, err := (&Compiler{}).Compile(request)
	if err != nil {
		t.Fatal(err)
	}
	if result.Root.Current != nil || result.Root.Sequence != nil || result.Root.Write.Existing != plan.ActionUpdate || result.Root.Write.Missing != plan.ActionUpdate || len(result.Root.Write.Allowed) != 1 || result.Root.Write.Allowed[0] != plan.ActionUpdate {
		t.Fatalf("command plan: %+v", result.Root)
	}
	after, _ := json.Marshal(component)
	if string(before) != string(after) {
		t.Fatal("authored metadata mutated")
	}
	request.Current = "Prior"
	if _, err = (&Compiler{}).Compile(request); err == nil {
		t.Fatal("command accepted Current")
	}
}
func TestCompilerUpdateCommandAdmission(t *testing.T) {
	for _, test := range []struct {
		name      string
		change    func(*spec.View, *plan.RecordPlan)
		operation plan.Operation
	}{
		{"POST", func(*spec.View, *plan.RecordPlan) {}, plan.OperationPost},
		{"PUT", func(*spec.View, *plan.RecordPlan) {}, plan.OperationPut},
		{"auxiliary", func(v *spec.View, r *plan.RecordPlan) { r.Auxiliary = true }, plan.OperationPatch},
		{"Current", func(v *spec.View, r *plan.RecordPlan) { r.Current = &plan.CurrentPlan{} }, plan.OperationPatch},
		{"sequence", func(v *spec.View, r *plan.RecordPlan) { r.Sequence = &plan.SequencePlan{} }, plan.OperationPatch},
		{"keyless", func(v *spec.View, r *plan.RecordPlan) { r.Keys = nil }, plan.OperationPatch},
		{"relation", func(v *spec.View, r *plan.RecordPlan) { v.Relations = []*spec.Relation{{Name: "Child"}} }, plan.OperationPatch},
		{"delete", func(v *spec.View, r *plan.RecordPlan) { r.Write.DeleteMarker = plan.FieldRef{Field: "Remove"} }, plan.OperationPatch},
		{"concurrency", func(v *spec.View, r *plan.RecordPlan) { r.Write.ConcurrencyToken = plan.FieldRef{Field: "Version"} }, plan.OperationPatch},
		{"identity", func(v *spec.View, r *plan.RecordPlan) { v.WriterIdentityPolicy = "assigned-update" }, plan.OperationPatch},
		{"predicate", func(v *spec.View, r *plan.RecordPlan) { v.MutationPredicateGroup = new(int) }, plan.OperationPatch},
		{"queue contract", func(v *spec.View, r *plan.RecordPlan) { v.QueueContract = "source-row" }, plan.OperationPatch},
	} {
		t.Run(test.name, func(t *testing.T) {
			v := &spec.View{WriterActionPolicy: "update"}
			r := &plan.RecordPlan{Table: "records", Keys: []plan.KeyPart{{Field: "ID"}}}
			test.change(v, r)
			if err := compileWriterActionPolicy(r, v, test.operation); err == nil {
				t.Fatal("incompatible command accepted")
			}
		})
	}
}
