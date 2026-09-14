package velty

import (
	"github.com/viant/datly/spec"
	plan "github.com/viant/datly/transcribe/handler/ast"
	"testing"
)

func TestAuxiliaryRootWithoutCurrentKeepsBusinessOutput(t *testing.T) {
	actual, err := Render(&plan.Plan{Operation: plan.OperationPatch, Root: &plan.RecordPlan{Auxiliary: true, InputPath: plan.FieldPath{"Input", "Events"}, Cardinality: spec.CardinalityMany}, Output: &plan.ContractRef{Path: plan.FieldPath{"Output", "Data"}}})
	if err != nil || actual != "#set($Output.Data = $Input.Events)" {
		t.Fatalf("read-only output=%q err=%v", actual, err)
	}
}
