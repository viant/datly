package compiler

import (
	"github.com/viant/datly/spec"
	plan "github.com/viant/datly/transcribe/handler/ast"
	"strings"
	"testing"
)

func mergeAuthority() map[string]plan.StatementSelector {
	a := statementAuthority()
	a["Input.Table"] = plan.StatementSelector{Path: plan.FieldPath{"Input", "Table"}, Type: spec.TypeRef{Name: "string"}, Addressable: true}
	a["Input.Config"] = plan.StatementSelector{Path: plan.FieldPath{"Input", "Config"}, Type: spec.TypeRef{Name: "MergeConfig", Package: "github.com/viant/sqlx/metadata/info"}, Addressable: true}
	a["Output.Result"] = plan.StatementSelector{Path: plan.FieldPath{"Output", "Result"}, Type: spec.TypeRef{Name: "MergeResult", Package: "github.com/viant/sqlx/metadata/info"}, Addressable: true}
	return a
}
func TestCompileBufferedMergeUsesCanonicalSQLXAuthority(t *testing.T) {
	const src = `$dml.Merge($Input.Table, $Input.Rows, $Input.Config, $Output.Result)`
	a := mergeAuthority()
	got, err := (&Compiler{}).CompileStatement(src, a)
	if err != nil {
		t.Fatal(err)
	}
	if got.Method != "Merge" || got.SQL != "" || got.MergeRows.Type.Name != "[]*Record" {
		t.Fatalf("unexpected merge plan: %+v", got)
	}
	a["Input.Rows"].Path[1] = "changed"
	if got.MergeRows.Path[1] != "Rows" {
		t.Fatal("plan retains mutable contract path")
	}
	for _, source := range []string{strings.Replace(src, "$Input.Table", `"records"`, 1), strings.Replace(src, "$Input.Rows", "$Input.Name", 1), strings.Replace(src, "$Input.Config", "$Input.Config.Build()", 1), src + "\n" + src, strings.Replace(src, "$Output.Result", "$Input.ID", 1), strings.Replace(src, "$Output.Result", "$Output.Result, $Input.ID", 1)} {
		if _, err := (&Compiler{}).CompileStatement(source, mergeAuthority()); err == nil {
			t.Fatalf("unsupported merge admitted: %s", source)
		}
	}
	for _, key := range []string{"Input.Config", "Output.Result"} {
		a := mergeAuthority()
		field := a[key]
		field.Type.Package = "example.com/fake/info"
		a[key] = field
		if _, err := (&Compiler{}).CompileStatement(src, a); err == nil {
			t.Fatalf("foreign SQLX names admitted: %s", key)
		}
	}
	a = mergeAuthority()
	field := a["Output.Result"]
	field.Addressable = false
	a["Output.Result"] = field
	if _, err := (&Compiler{}).CompileStatement(src, a); err == nil {
		t.Fatal("non-addressable result admitted")
	}
}
