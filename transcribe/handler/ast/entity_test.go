package ast

import "testing"

func TestEntityPlanCloneDetachesInvariantFields(t *testing.T) {
	input := &EntityPlan{Fields: []EntityField{{Name: "Start", Invariant: "Schedule"}}, Invariants: []InvariantGroup{{Name: "Schedule", Fields: []string{"Start", "End"}}}}
	cloned := input.Clone()
	cloned.Fields[0].Invariant = "Changed"
	cloned.Invariants[0].Name = "Changed"
	cloned.Invariants[0].Fields[0] = "Changed"
	if input.Fields[0].Invariant != "Schedule" || input.Invariants[0].Name != "Schedule" || input.Invariants[0].Fields[0] != "Start" {
		t.Fatal("entity clone retained mutable invariant aliases")
	}
}
