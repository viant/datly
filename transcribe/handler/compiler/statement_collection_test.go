package compiler

import (
	"reflect"
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
		if len(result.CollectionOrdinals) != 1 || result.CollectionOrdinals[0] != 1 {
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
		if len(result.CollectionOrdinals) != 1 || result.CollectionOrdinals[0] != 1 {
			t.Fatal("protected SQL shifted ordinal")
		}
	}
	for _, sql := range []string{"UPDATE records SET name=? WHERE id NOT IN (?)", "UPDATE records SET name=? WHERE id IN (?,2)", "UPDATE records SET name=? WHERE id=?", "UPDATE records SET name=? WHERE (id,owner_id) IN (?)", "UPDATE records SET name=? WHERE NOT (id IN (?))"} {
		if _, err := (&Compiler{}).CompileStatement(`$dml.Execute("`+sql+`", $Input.Name, $Input.IDs)`, authority); err == nil {
			t.Fatalf("admitted %s", sql)
		}
	}
}

func TestStatementMultipleCollections(t *testing.T) {
	authority := statementAuthority()
	for _, name := range []string{"IDs", "Kinds"} {
		authority["Input."+name] = plan.StatementSelector{Path: plan.FieldPath{"Input", name}, Type: spec.TypeRef{Name: "[]int"}}
	}
	for _, tc := range []struct {
		name, sql, args string
		ordinals        []int
	}{
		{"delete cross product", "DELETE FROM records WHERE id IN (?) AND kind IN (?)", ", $Input.IDs, $Input.Kinds", []int{0, 1}},
		{"qualified parenthesized delete", "DELETE FROM records WHERE (records.id IN (?) AND (records.kind IN (?)))", ", $Input.IDs, $Input.Kinds", []int{0, 1}},
		{"mixed scalars", "DELETE FROM records WHERE owner_id=? AND id IN (?) AND name=? AND kind IN (?) AND category_id=?", ", $Input.OwnerID, $Input.IDs, $Input.Name, $Input.Kinds, $Input.CategoryID", []int{1, 3}},
		{"multiple update", "UPDATE records SET name=? WHERE id IN (?) AND kind IN (?)", ", $Input.Name, $Input.IDs, $Input.Kinds", []int{1, 2}},
		{"protected placeholders and marker collision", "DELETE FROM records WHERE name='?' AND id IN (?) /* ? :datlyNativeCollection_ */ AND owner_id=:datlyNativeCollection AND kind IN (?)", ", $Input.IDs, $Input.Kinds", []int{0, 1}},
		{"same selector twice", "DELETE FROM records WHERE id IN (?) OR owner_id IN (?)", ", $Input.IDs, $Input.IDs", []int{0, 1}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			result, err := (&Compiler{}).CompileStatement(`$dml.Execute("`+tc.sql+`"`+tc.args+`)`, authority)
			if err != nil {
				t.Fatal(err)
			}
			if result.SQL != tc.sql {
				t.Fatal("authored SQL changed")
			}
			if !reflect.DeepEqual(result.CollectionOrdinals, tc.ordinals) {
				t.Fatalf("collection order: %+v", result.CollectionOrdinals)
			}
		})
	}
}

func TestStatementDeleteCollectionBoundaries(t *testing.T) {
	authority := statementAuthority()
	authority["Input.IDs"] = plan.StatementSelector{Path: plan.FieldPath{"Input", "IDs"}, Type: spec.TypeRef{Name: "[]int"}}
	for _, sql := range []string{
		"DELETE FROM records WHERE id NOT IN (?)",
		"DELETE FROM records WHERE NOT (id IN (?))",
		"DELETE FROM records WHERE id IN (?,2)",
		"DELETE FROM records WHERE id=?",
		"DELETE FROM records WHERE (id,owner_id) IN (?)",
		"DELETE FROM records WHERE abs(id) IN (?)",
		"DELETE FROM records WHERE ? IN (id)",
		"INSERT INTO records(id) VALUES (?)",
		"UPDATE records SET name=? WHERE id=1",
	} {
		t.Run(sql, func(t *testing.T) {
			if _, err := (&Compiler{}).CompileStatement(`$dml.Execute("`+sql+`", $Input.IDs)`, authority); err == nil {
				t.Fatal("unsupported collection form admitted")
			}
		})
	}
	if _, err := (&Compiler{}).CompileStatement(`$dml.Execute("DELETE FROM records WHERE id IN (?,?)", $Input.IDs, $Input.IDs)`, authority); err == nil {
		t.Fatal("multiple bindings in one IN admitted")
	}
	if _, err := (&Compiler{}).CompileStatement(`$dml.ExecuteWithResult("INSERT INTO records(id) VALUES (?)", $Input.ID, $Input.IDs)`, authority); err == nil {
		t.Fatal("collection acquired inserted result semantics")
	}
}
