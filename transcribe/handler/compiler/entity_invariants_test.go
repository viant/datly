package compiler

import (
	"reflect"
	"testing"

	plan "github.com/viant/datly/transcribe/handler/ast"
)

func TestEntityInvariants(t *testing.T) {
	for _, test := range []struct {
		name    string
		fields  []plan.EntityField
		want    []plan.InvariantGroup
		invalid bool
	}{
		{name: "unbounded members", fields: []plan.EntityField{{Name: "Start", Invariant: "work_schedule", Writable: true}, {Name: "End", Invariant: "work_schedule", Writable: true}, {Name: "Zone", Invariant: "work_schedule", Writable: true}, {Name: "Days", Invariant: "work_schedule", Writable: true}}, want: []plan.InvariantGroup{{Name: "WorkSchedule", Fields: []string{"Start", "End", "Zone", "Days"}}}},
		{name: "collision", fields: []plan.EntityField{{Name: "A", Invariant: "work_schedule", Writable: true}, {Name: "B", Invariant: "WorkSchedule", Writable: true}}, invalid: true},
		{name: "identity", fields: []plan.EntityField{{Name: "ID", Invariant: "Schedule", Writable: true, Identity: true}}, invalid: true},
		{name: "relation", fields: []plan.EntityField{{Name: "Children", Invariant: "Schedule", Writable: true, Relation: true}}, invalid: true},
		{name: "read only", fields: []plan.EntityField{{Name: "Start", Invariant: "Schedule"}}, invalid: true},
		{name: "multiple groups", fields: []plan.EntityField{{Name: "Start", Invariant: "A,B", Writable: true}}, invalid: true},
	} {
		t.Run(test.name, func(t *testing.T) {
			input := &plan.EntityPlan{Fields: test.fields}
			actual, err := EntityInvariants(input)
			if (err != nil) != test.invalid {
				t.Fatalf("error = %v", err)
			}
			if !test.invalid && !reflect.DeepEqual(actual, test.want) {
				t.Fatalf("groups = %#v", actual)
			}
			if input.Invariants != nil {
				t.Fatal("compiler mutated input")
			}
		})
	}
}
