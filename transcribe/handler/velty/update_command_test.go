package velty

import (
	plan "github.com/viant/datly/transcribe/handler/ast"
	"strings"
	"testing"
)

func TestUpdateCommandRejectsVelty(t *testing.T) {
	_, err := Render(&plan.Plan{Operation: plan.OperationPatch, Root: &plan.RecordPlan{InputPath: plan.FieldPath{"Input", "Rows"}, Write: plan.WritePolicy{ActionPolicy: "update"}}})
	if err == nil || !strings.Contains(err.Error(), "Velty is unsupported") {
		t.Fatalf("unsupported command accepted: %v", err)
	}
}
