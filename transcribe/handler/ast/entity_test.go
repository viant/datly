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

func TestLookupProjectionCloneIsDetached(t *testing.T) {
	source := &Plan{Root: &RecordPlan{Current: &CurrentPlan{Lookup: &LookupProjection{Name: "Keys", Columns: []string{"ID"}}}}}
	clone := source.Clone()
	clone.Root.Current.Lookup.Columns[0] = "OTHER"
	if source.Root.Current.Lookup.Columns[0] != "ID" {
		t.Fatal("lookup columns aliased across semantic clone")
	}
}
