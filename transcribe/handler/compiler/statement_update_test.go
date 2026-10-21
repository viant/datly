package compiler

import (
	"github.com/viant/datly/spec"
	plan "github.com/viant/datly/transcribe/handler/ast"
	"testing"
)

func TestCompileTypedUpdate(t *testing.T) {
	authority := map[string]plan.StatementSelector{
		"Input.Table": {Path: plan.FieldPath{"Input", "Table"}, Type: spec.TypeRef{Name: "string"}},
		"Input.Row":   {Path: plan.FieldPath{"Input", "Row"}, Type: spec.TypeRef{Name: "*model.Record"}},
	}
	const source = `$dml.Update($Input.Table,$Input.Row)`
	got, err := (&Compiler{}).CompileStatement(source, authority)
	if err != nil {
		t.Fatal(err)
	}
	if got.Method != "Update" || len(got.Arguments) != 2 || got.Arguments[1].Type.Name != "*model.Record" {
		t.Fatalf("plan=%+v", got)
	}
	for _, bad := range []string{`$dml.Update($Input.Table)`, `$dml.Update($Input.Table,$Input.Row,1)`, `$dml.Update($Input.Missing,$Input.Row)`, `$dml.Update($Input.Table,$Input.Missing)`, `$dml.Update($Input.Table,$Input.Table)`} {
		if _, err := (&Compiler{}).CompileStatement(bad, authority); err == nil {
			t.Fatalf("accepted %s", bad)
		}
	}
	for _, typ := range []string{"*Record", "*dealRecord", "*model.Record", "*example.com/app/model.Record"} {
		field := authority["Input.Row"]
		field.Type.Name = typ
		authority["Input.Row"] = field
		if _, err := (&Compiler{}).CompileStatement(source, authority); err != nil {
			t.Fatalf("%s: %v", typ, err)
		}
	}
}
