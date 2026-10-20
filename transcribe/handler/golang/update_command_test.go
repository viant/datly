package golang

import (
	"strings"
	"testing"

	plan "github.com/viant/datly/transcribe/handler/ast"
)

func updateCommandPlan() *plan.Plan {
	semantic := rootSemanticPlan(plan.OperationPatch, false)
	semantic.Root.Current = nil
	semantic.Root.Sequence = nil
	semantic.Root.Write.ActionPolicy = "update"
	semantic.Root.Write.Existing, semantic.Root.Write.Missing = plan.ActionUpdate, plan.ActionUpdate
	semantic.Root.Write.Allowed = []plan.Action{plan.ActionUpdate}
	return semantic
}
func TestLowerUpdateCommandDirectTypedDML(t *testing.T) {
	semantic := updateCommandPlan()
	config := Config{Package: "events", PackagePath: "example.com/events", Factory: "NewEventsHandler", InputType: "Input", OutputType: "Output", Records: rootRecordTypes(semantic, "[]*Event", "")}
	asset, err := Lower(semantic, config)
	if err != nil {
		t.Fatal(err)
	}
	source, err := asset.Source()
	if err != nil {
		t.Fatal(err)
	}
	text := string(source)
	if !strings.Contains(text, `dependencies.DML.Update("EVENTS", record0)`) {
		t.Fatal("typed update absent")
	}
	for _, forbidden := range []string{"dependencies.DML.Insert", "dependencies.DML.Delete", ".Allocate(", "currentByKey", "recordExists", "missing generated PATCH key"} {
		if strings.Contains(text, forbidden) {
			t.Fatalf("command contains %q", forbidden)
		}
	}
	compileGeneratedHandler(t, source, `package events
 type Event struct {Id *int64;Name string}
 type Input struct {Events []*Event}
 type Output struct {Data []*Event}
 `)
}
func TestUpdateCommandRejectsLegacyMutationProducts(t *testing.T) {
	semantic := updateCommandPlan()
	config := Config{Package: "events", PackagePath: "example.com/events", Factory: "NewEventsHandler", InputType: "Input", OutputType: "Output", Records: rootRecordTypes(semantic, "[]*Event", "")}
	_, err := MutationProgram(semantic, config)
	if err == nil || !strings.Contains(err.Error(), "component-specific mutation programs are unsupported") {
		t.Fatalf("MutationProgram: %v", err)
	}
	_, err = MutationActionSupport(semantic, config, nil, nil)
	if err == nil || !strings.Contains(err.Error(), "component-specific mutation programs are unsupported") {
		t.Fatalf("MutationActionSupport: %v", err)
	}
	_, err = MutationValidationSupport(semantic, config, nil, nil)
	if err == nil || !strings.Contains(err.Error(), "component-specific mutation programs are unsupported") {
		t.Fatalf("MutationValidationSupport: %v", err)
	}
}
