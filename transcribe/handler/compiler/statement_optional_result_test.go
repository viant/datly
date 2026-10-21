package compiler

import (
	"strings"
	"testing"
)

func TestOptionalResultStatementRules(t *testing.T) {
	a := statementAuthority()
	valid := `$dml.ExecuteWithOptionalResult("INSERT INTO records(name) VALUES (?)", $Input.OwnerID, $Input.Name)`
	got, err := (&Compiler{}).CompileStatement(valid, a)
	if err != nil {
		t.Fatal(err)
	}
	if got.Method != "ExecuteWithOptionalResult" || len(got.LastInsertID.Path) != 2 {
		t.Fatal("optional result authority lost")
	}
	for _, source := range []string{strings.Replace(valid, "INSERT INTO records(name) VALUES (?)", "UPDATE records SET name=?", 1), strings.Replace(valid, "$Input.OwnerID", "$Input.Name", 1), strings.Replace(valid, "VALUES (?)", "VALUES (?),(?)", 1)} {
		if _, err := (&Compiler{}).CompileStatement(source, a); err == nil {
			t.Fatal("invalid optional result admitted")
		}
	}
}
