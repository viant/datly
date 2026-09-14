package transcribe

import (
	"reflect"
	"testing"

	gen "github.com/viant/datly/transcribe/generate"
	plan "github.com/viant/datly/transcribe/handler/ast"
)

func TestGeneratedEntityInvariantMetadata(t *testing.T) {
	for _, test := range []struct {
		name, tag string
		invalid   bool
	}{
		{"single group", `invariant:"Schedule"`, false},
		{"multiple groups", `invariant:"Schedule,Other"`, true},
		{"empty", `invariant:""`, true},
	} {
		t.Run(test.name, func(t *testing.T) {
			semantic := &plan.Plan{Operation: plan.OperationPatch, Root: &plan.RecordPlan{Identity: "view:Records"}}
			view := gen.ViewPlan{Identity: semantic.Root.Identity, Name: "Record", Ownership: gen.ViewGenerated}
			for _, name := range []string{"Start", "End", "Zone", "Days"} {
				view.SetMarkerFields = append(view.SetMarkerFields, name)
				view.Fields = append(view.Fields, gen.Field{Name: name, Type: "string", Tag: test.tag})
			}
			result, err := newHandlerGeneration(nil, nil, Options{}).withGeneratedPresence(semantic, &gen.Plan{Views: []gen.ViewPlan{view}})
			if (err != nil) != test.invalid {
				t.Fatalf("generation metadata error = %v", err)
			}
			if err != nil {
				return
			}
			want := []plan.InvariantGroup{{Name: "Schedule", Fields: []string{"Start", "End", "Zone", "Days"}}}
			if !reflect.DeepEqual(result.Root.Entity.Invariants, want) {
				t.Fatalf("invariants = %#v", result.Root.Entity.Invariants)
			}
			if semantic.Root.Entity != nil {
				t.Fatal("canonical semantic plan was mutated")
			}
		})
	}
}
