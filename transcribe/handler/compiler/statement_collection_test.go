package compiler

import (
	"testing"

	"github.com/viant/datly/spec"
	plan "github.com/viant/datly/transcribe/handler/ast"
)

func TestStatementCollection(t *testing.T) {
	authority := statementAuthority()
	authority["Input.IDs"] = plan.StatementSelector{Path: plan.FieldPath{"Input", "IDs"}, Type: spec.TypeRef{Name: "[]int"}}
	for _, predicate := range []string{"id IN (?)", "id IN (?) AND owner_id=?"} {
		args := ", $Input.Name, $Input.IDs"
		if predicate != "id IN (?)" {
			args += ", $Input.OwnerID"
		}
		result, err := (&Compiler{}).CompileStatement(`$dml.Execute("UPDATE records SET name=? WHERE `+predicate+`"`+args+`)`, authority)
		if err != nil {
			t.Fatal(err)
		}
		if result.CollectionOrdinal == nil || *result.CollectionOrdinal != 1 {
			t.Fatalf("ordinal: %+v", result)
		}
	}

	for _, sql := range []string{
		"UPDATE records SET name=?, note='?' WHERE id IN (?) /* ? */",
		"UPDATE records SET name=? WHERE id IN (?) AND owner_id=:owner",
		"UPDATE records SET name=? WHERE id IN (?) AND owner_id=:datlyNativeCollection",
	} {
		result, err := (&Compiler{}).CompileStatement(`$dml.Execute("`+sql+`", $Input.Name, $Input.IDs)`, authority)
		if err != nil {
			t.Fatal(err)
		}
		if result.SQL != sql {
			t.Fatal("authored SQL changed")
		}
		if result.CollectionOrdinal == nil || *result.CollectionOrdinal != 1 {
			t.Fatal("protected SQL shifted ordinal")
		}
	}
	for _, sql := range []string{"UPDATE records SET name=? WHERE id NOT IN (?)", "UPDATE records SET name=? WHERE id IN (?,2)", "UPDATE records SET name=? WHERE id=?", "UPDATE records SET name=? WHERE (id,owner_id) IN (?)", "UPDATE records SET name=? WHERE NOT (id IN (?))"} {
		if _, err := (&Compiler{}).CompileStatement(`$dml.Execute("`+sql+`", $Input.Name, $Input.IDs)`, authority); err == nil {
			t.Fatalf("admitted %s", sql)
		}
	}
}
