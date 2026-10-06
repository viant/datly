package velty

import (
	"github.com/viant/datly/spec"
	plan "github.com/viant/datly/transcribe/handler/ast"
	"strings"
	"testing"
)

func TestAuxiliaryAllocationRejectsExplicitExecutableMetadata(t *testing.T) {
	for _, policy := range []string{"sequence", "existing", "missing", "allowed"} {
		t.Run(policy, func(t *testing.T) {
			root := &plan.RecordPlan{Auxiliary: true, InputPath: plan.FieldPath{"Input", "Events"}, Cardinality: spec.CardinalityMany}
			switch policy {
			case "sequence":
				root.Sequence = &plan.SequencePlan{}
			case "existing":
				root.Write.Existing = plan.ActionUpdate
			case "missing":
				root.Write.Missing = plan.ActionInsert
			case "allowed":
				root.Write.Allowed = []plan.Action{plan.ActionInsert}
			}
			_, err := Render(&plan.Plan{Operation: plan.OperationPatch, Root: root, Output: &plan.ContractRef{Path: plan.FieldPath{"Output", "Data"}}})
			if err == nil || !strings.Contains(err.Error(), "mutation operations") {
				t.Fatalf("explicit auxiliary executable policy admitted: %v", err)
			}
		})
	}
}
