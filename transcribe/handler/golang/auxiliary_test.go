package golang

import (
	plan "github.com/viant/datly/transcribe/handler/ast"
	"strings"
	"testing"
)

func TestAuxiliaryRootWithoutCurrentOrMutationCompiles(t *testing.T) {
	semantic := rootSemanticPlan(plan.OperationPatch, false)
	semantic.Root.Auxiliary = true
	semantic.Root.Current = nil
	semantic.Root.Sequence = nil
	semantic.Root.Table = ""
	semantic.Root.Keys = nil
	semantic.Root.Write = plan.WritePolicy{ValuePath: semantic.Root.InputPath}
	asset, err := Lower(semantic, Config{Package: "events", Factory: "NewEventsHandler", InputType: "Input", OutputType: "Output", Records: rootRecordTypes(semantic, "[]*Event", "")})
	if err != nil {
		t.Fatal(err)
	}
	source, err := asset.Source()
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(source), "dependencies.DML") || strings.Contains(string(source), "Sequencer") {
		t.Fatalf("read-only mutation source: %s", source)
	}
	compileGeneratedHandler(t, source, "package events\ntype Event struct{Name string}\ntype Input struct{Events []*Event}\ntype Output struct{Data []*Event}\n")
}
