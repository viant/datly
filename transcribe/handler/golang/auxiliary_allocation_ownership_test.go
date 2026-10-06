package golang

import (
	plan "github.com/viant/datly/transcribe/handler/ast"
	"strings"
	"testing"
)

func TestAuxiliaryAllocationRejectsExplicitExecutableMetadata(t *testing.T) {
	for _, policy := range []string{"sequence", "existing", "missing", "allowed"} {
		t.Run(policy, func(t *testing.T) {
			semantic := rootSemanticPlan(plan.OperationPatch, false)
			root := semantic.Root
			root.Auxiliary = true
			root.Current = nil
			sequence := root.Sequence
			root.Sequence = nil
			root.Write = plan.WritePolicy{ValuePath: root.InputPath}
			switch policy {
			case "sequence":
				root.Sequence = sequence
			case "existing":
				root.Write.Existing = plan.ActionUpdate
			case "missing":
				root.Write.Missing = plan.ActionInsert
			case "allowed":
				root.Write.Allowed = []plan.Action{plan.ActionInsert}
			}
			_, err := Lower(semantic, Config{Package: "events", Factory: "NewEventsHandler", InputType: "Input", OutputType: "Output", Records: rootRecordTypes(semantic, "[]*Event", "")})
			if err == nil || !strings.Contains(err.Error(), "mutation operations") {
				t.Fatalf("explicit auxiliary executable policy admitted: %v", err)
			}
		})
	}
}
