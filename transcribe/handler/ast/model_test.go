package ast

import "testing"

func TestRecordPlanTracksPresence(t *testing.T) {
	tests := []struct {
		name   string
		record *RecordPlan
		field  string
		want   bool
	}{
		{name: "nil record", field: "ID"},
		{name: "empty", record: &RecordPlan{}, field: "ID"},
		{name: "exact field", record: &RecordPlan{PresenceFields: []string{"ID", "TenantID"}}, field: "TenantID", want: true},
		{name: "case remains semantic", record: &RecordPlan{PresenceFields: []string{"TenantID"}}, field: "tenantID"},
	}
	for _, testCase := range tests {
		t.Run(testCase.name, func(t *testing.T) {
			if actual := testCase.record.TracksPresence(testCase.field); actual != testCase.want {
				t.Fatalf("TracksPresence(%q) = %v, want %v", testCase.field, actual, testCase.want)
			}
		})
	}
}

func TestPlanCloneDetachesSemanticTree(t *testing.T) {
	child := &RecordPlan{Identity: "child", InputPath: FieldPath{"Input", "Children"}, PresenceFields: []string{"ParentID"}}
	source := &Plan{
		Operation: OperationPatch,
		Input:     ContractRef{Path: FieldPath{"Input", "Parents"}},
		Output:    &ContractRef{Path: FieldPath{"Output", "Data"}},
		Root: &RecordPlan{
			Identity: "root", InputPath: FieldPath{"Input", "Parents"}, Keys: []KeyPart{{Field: "ID"}},
			Current:   &CurrentPlan{InputPath: FieldPath{"Input", "CurrentParents"}, Keys: []KeyPart{{Field: "ID"}}},
			Sequence:  &SequencePlan{Destination: FieldPath{"Input", "Parents"}, Selector: FieldPath{"ID"}},
			Write:     WritePolicy{ValuePath: FieldPath{"Input", "Parents"}, Allowed: []Action{ActionInsert, ActionUpdate}},
			Relations: []*RelationPlan{{FieldPath: FieldPath{"Children"}, Links: []KeyLink{{Child: KeyPart{Field: "ParentID"}}}, Child: child}},
		},
	}

	cloned := source.Clone()
	cloned.Input.Path[0] = "Changed"
	cloned.Output.Path[0] = "Changed"
	cloned.Root.Keys[0].Field = "Changed"
	cloned.Root.Current.InputPath[0] = "Changed"
	cloned.Root.Sequence.Selector[0] = "Changed"
	cloned.Root.Write.Allowed[0] = ActionUpdate
	cloned.Root.Relations[0].Links[0].Child.Field = "Changed"
	cloned.Root.Relations[0].Child.PresenceFields[0] = "Changed"

	if source.Input.Path[0] != "Input" || source.Output.Path[0] != "Output" || source.Root.Keys[0].Field != "ID" ||
		source.Root.Current.InputPath[0] != "Input" || source.Root.Sequence.Selector[0] != "ID" ||
		source.Root.Write.Allowed[0] != ActionInsert || source.Root.Relations[0].Links[0].Child.Field != "ParentID" ||
		child.PresenceFields[0] != "ParentID" {
		t.Fatalf("Clone() retained mutable source aliases: source=%+v child=%+v", source, child)
	}
}
