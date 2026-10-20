package compiler

import (
	"github.com/viant/datly/spec"
	plan "github.com/viant/datly/transcribe/handler/ast"
	"strings"
	"testing"
)

func batchAuthority() map[string]plan.StatementSelector {
	a := mergeAuthority()
	a["Input.BatchSize"] = plan.StatementSelector{Path: plan.FieldPath{"Input", "BatchSize"}, Type: spec.TypeRef{Name: "int"}, Addressable: true}
	a["Output.Affected"] = plan.StatementSelector{Path: plan.FieldPath{"Output", "Affected"}, Type: spec.TypeRef{Name: "int64"}, Addressable: true}
	return a
}
func TestCompileBatchStatementCanonicalContracts(t *testing.T) {
	for _, method := range []string{"Insert", "Delete", "InsertIgnore"} {
		source := `$dml.` + method + `($Input.Table,$Input.Rows,$Input.BatchSize,$Output.Affected)`
		got, err := (&Compiler{}).CompileStatement(source, batchAuthority())
		if err != nil {
			t.Fatal(err)
		}
		if got.Method != method || got.SQL != "" || got.BatchRows.Type.Name != "[]*Record" {
			t.Fatalf("plan=%+v", got)
		}
		for _, bad := range []string{strings.Replace(source, "$Input.Table", `"mapping"`, 1), strings.Replace(source, "$Input.Rows", "$Input.Name", 1), strings.Replace(source, "$Input.BatchSize", "8000", 1), strings.Replace(source, "$Output.Affected", "$Input.ID", 1), source + "\n" + source} {
			if _, err = (&Compiler{}).CompileStatement(bad, batchAuthority()); err == nil {
				t.Fatalf("unsupported batch admitted: %s", bad)
			}
		}
		a := batchAuthority()
		field := a["Output.Affected"]
		field.Addressable = false
		a["Output.Affected"] = field
		if _, err = (&Compiler{}).CompileStatement(source, a); err == nil {
			t.Fatal("nonaddressable result accepted")
		}
	}
}

func TestCompileBatchInsertLiteralConflictSuffix(t *testing.T) {
	const suffix = "ON DUPLICATE KEY UPDATE WEIGHT = VALUES(WEIGHT)"
	prefix := `$dml.Insert($Input.Table,$Input.Rows,$Input.BatchSize,$Output.Affected`
	got, err := (&Compiler{}).CompileStatement(prefix+`,"`+suffix+`")`, batchAuthority())
	if err != nil {
		t.Fatal(err)
	}
	if got.BatchOnDuplicateKeySQL == nil || *got.BatchOnDuplicateKeySQL != suffix {
		t.Fatalf("suffix=%v", got.BatchOnDuplicateKeySQL)
	}
	for _, source := range []string{prefix + `,$Input.Table)`, prefix + `,1)`, prefix + `,"suffix","extra")`, strings.Replace(prefix, "Insert", "Delete", 1) + `,"suffix")`, strings.Replace(prefix, "Insert", "InsertIgnore", 1) + `,"suffix")`, `$dml.Insert($Input.Table,$Input.Rows,$Input.BatchSize)`} {
		if _, err = (&Compiler{}).CompileStatement(source, batchAuthority()); err == nil {
			t.Fatalf("invalid suffix contract accepted: %s", source)
		}
	}
}
