package golang

import (
	"bytes"
	"github.com/viant/datly/spec"
	plan "github.com/viant/datly/transcribe/handler/ast"
	"go/format"
	"go/token"
	"strings"
	"testing"
)

func TestLowerTypedUpdatePreservesSelectorsAndGuards(t *testing.T) {
	p := &plan.StatementPlan{Method: "Update", Arguments: []plan.StatementSelector{
		{Path: plan.FieldPath{"Input", "Params", "Table"}, Type: spec.TypeRef{Name: "string"}, PointerParents: []plan.FieldPath{{"Input", "Params"}}},
		{Path: plan.FieldPath{"Input", "Params", "Row"}, Type: spec.TypeRef{Name: "*Record"}, PointerParents: []plan.FieldPath{{"Input", "Params"}}},
	}}
	file, err := StatementQueue(p, Config{Package: "example", Factory: "NewRecord", InputType: "Input", OutputType: "Output"})
	if err != nil {
		t.Fatal(err)
	}
	var b bytes.Buffer
	if err := format.Node(&b, token.NewFileSet(), file); err != nil {
		t.Fatal(err)
	}
	s := b.String()
	for _, expected := range []string{"Update(string, any) error", "return capability.Update(input.Params.Table, input.Params.Row)", "if input == nil || output == nil", "if input.Params == nil"} {
		if !strings.Contains(s, expected) {
			t.Fatalf("missing %q: %s", expected, s)
		}
	}
	if strings.Count(s, "if input.Params == nil") != 1 || strings.Contains(s, "input.Params.Row == nil") || strings.Contains(s, "Execute(") || strings.Contains(s, "WithBatchSize") {
		t.Fatalf("payload/guard contract changed: %s", s)
	}
	if strings.Index(s, "if input.Params == nil") > strings.Index(s, "data.(interface") {
		t.Fatal("parent guard follows capability")
	}
}
