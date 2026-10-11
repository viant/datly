package compiler

import (
	"reflect"
	"strings"
	"testing"

	"github.com/viant/datly/spec"
	plan "github.com/viant/datly/transcribe/handler/ast"
)

func statementAuthority() map[string]plan.StatementSelector {
	result := map[string]plan.StatementSelector{}
	for name, typ := range map[string]string{"ID": "int64", "OwnerID": "int", "CategoryID": "int", "EntityID": "int", "Now": "time.Time", "Name": "string", "Rows": "[]*Record", "UnsignedID": "uint64"} {
		result["Input."+name] = plan.StatementSelector{Path: plan.FieldPath{"Input", name}, Type: spec.TypeRef{Name: typ}, Addressable: true}
	}
	return result
}

func TestCompileBufferedStatementResult(t *testing.T) {
	const sql = "INSERT INTO records(owner_id,category_id,entity_id,created,updated) VALUES (?,?,?,?,?) ON DUPLICATE KEY UPDATE updated=VALUES(updated),id=LAST_INSERT_ID(id)"
	source := `$dml.ExecuteWithResult("` + sql + `", $Input.ID, $Input.OwnerID, $Input.CategoryID, $Input.EntityID, $Input.Now, $Input.Now)`
	result, err := (&Compiler{}).CompileStatement(source, statementAuthority())
	if err != nil {
		t.Fatal(err)
	}
	if result.SQL != sql || !reflect.DeepEqual(result.LastInsertID.Path, plan.FieldPath{"Input", "ID"}) || len(result.Arguments) != 5 {
		t.Fatalf("statement changed: %+v", result)
	}
	for i, name := range []string{"OwnerID", "CategoryID", "EntityID", "Now", "Now"} {
		if !reflect.DeepEqual(result.Arguments[i].Path, plan.FieldPath{"Input", name}) {
			t.Fatalf("argument %d reordered", i)
		}
	}
	// Semantic ownership is immutable even if a caller reuses its authority.
	a := statementAuthority()
	result, err = (&Compiler{}).CompileStatement(source, a)
	if err != nil {
		t.Fatal(err)
	}
	a["Input.ID"].Path[1] = "changed"
	if result.LastInsertID.Path[1] != "ID" {
		t.Fatal("statement retains mutable contract path")
	}
}

func TestCompileBufferedStatementRejectsUnsupportedPrograms(t *testing.T) {
	base := `$dml.ExecuteWithResult("INSERT INTO records(name) VALUES (?)", $Input.ID, $Input.Name)`
	for name, source := range map[string]string{
		"dynamic sql":         strings.Replace(base, `"INSERT INTO records(name) VALUES (?)"`, `$Input.Name`, 1),
		"multiple sql":        strings.Replace(base, "VALUES (?)", "VALUES (?); DELETE FROM records", 1),
		"runtime template":    strings.Replace(base, "VALUES (?)", "VALUES ($Input.Name)", 1),
		"transaction":         strings.Replace(base, "INSERT INTO records(name) VALUES (?)", "BEGIN", 1),
		"update identity":     strings.Replace(base, "INSERT INTO records(name) VALUES (?)", "UPDATE records SET name=?", 1),
		"unresolved":          strings.Replace(base, "$Input.Name", "$Input.Missing", 1),
		"collection":          strings.Replace(base, "$Input.Name", "$Input.Rows", 1),
		"index":               strings.Replace(base, "$Input.Name", "$Input.Rows[0].Name", 1),
		"function":            strings.Replace(base, "$Input.Name", "$Input.Name.ToLower()", 1),
		"literal argument":    strings.Replace(base, "$Input.Name", `"value"`, 1),
		"wrong binding count": strings.Replace(base, "VALUES (?)", "VALUES (?,?)", 1),
		"batch rows":          strings.Replace(base, "VALUES (?)", "VALUES (?),(?)", 1),
		"malformed SQL":       strings.Replace(base, "VALUES (?)", "VALUES (", 1),
		"unsigned result":     strings.Replace(base, "$Input.ID", "$Input.UnsignedID", 1),
		"multiple calls":      base + "\n" + base,
		"conditional":         `#if($Input.OwnerID)` + base + `#end`,
	} {
		t.Run(name, func(t *testing.T) {
			if _, err := (&Compiler{}).CompileStatement(source, statementAuthority()); err == nil {
				t.Fatal("unsupported authoring admitted")
			}
		})
	}
	a := statementAuthority()
	field := a["Input.ID"]
	field.Addressable = false
	a["Input.ID"] = field
	if _, err := (&Compiler{}).CompileStatement(base, a); err == nil {
		t.Fatal("non-addressable result admitted")
	}
}

func TestCompileBufferedStatementProtectedSQLLiterals(t *testing.T) {
	source := `$dml.ExecuteWithResult("INSERT INTO records(name,note) VALUES (?, '? $text # literal') /* ? $comment */", $Input.ID, $Input.Name)`
	if _, err := (&Compiler{}).CompileStatement(source, statementAuthority()); err != nil {
		t.Fatal(err)
	}
}

func TestCompileBufferedStatementExecute(t *testing.T) {
	for _, sql := range []string{
		"UPDATE records SET name='' WHERE owner_id=?",
		"DELETE FROM records WHERE owner_id=?",
		"INSERT INTO records(owner_id) VALUES (?)",
	} {
		result, err := (&Compiler{}).CompileStatement(`$dml.Execute("`+sql+`", $Input.OwnerID)`, statementAuthority())
		if err != nil {
			t.Fatal(err)
		}
		if result.Method != "Execute" || result.SQL != sql || len(result.Arguments) != 1 || len(result.LastInsertID.Path) != 0 {
			t.Fatalf("non-result queue changed: %+v", result)
		}
	}
	if _, err := (&Compiler{}).CompileStatement(`$dml.Execute("UPDATE records SET name='? $literal' /* ? */")`, statementAuthority()); err != nil {
		t.Fatal(err)
	}
	for _, source := range []string{
		`$dml.Execute("UPDATE records SET name=?", $Input.Missing)`,
		`$dml.Execute("UPDATE records SET name=?", $Input.Rows)`,
		`$dml.Execute("UPDATE records SET name=?")`,
		`$dml.Execute("UPDATE records SET name=''", $Input.Name)`,
		`$dml.Execute("UPDATE records SET name='' WHERE id=1; DELETE FROM records")`,
		`$dml.Execute("BEGIN")`,
		`$dml.Execute("CREATE TABLE records(id INT)")`,
		`$dml.Execute("UPDATE records SET name=$Input.Name")`,
		`$dml.Execute($Input.Name)`,
		`$dml.Execute("UPDATE records SET name=(")`,
	} {
		if _, err := (&Compiler{}).CompileStatement(source, statementAuthority()); err == nil {
			t.Fatalf("unsupported program admitted: %s", source)
		}
	}
}

func TestCompileBufferedStatementNativeBoundaries(t *testing.T) {
	for _, sql := range []string{
		"UPDATE records SET name='; ? $literal' WHERE owner_id=?; /* DELETE FROM records; */",
		"UPDATE records SET name='a' WHERE owner_id=?; -- DELETE FROM records;",
		"UPDATE records SET name='a' WHERE owner_id=? -- trailing comment",
		"UPDATE records SET name='a' WHERE owner_id=?; /* quoted ';' */ ;",
	} {
		result, err := (&Compiler{}).CompileStatement(`$dml.Execute("`+sql+`", $Input.OwnerID)`, statementAuthority())
		if err != nil {
			t.Fatal(err)
		}
		if result.SQL != sql {
			t.Fatalf("authored SQL changed: %q", result.SQL)
		}
	}
	for _, sql := range []string{
		"UPDATE records SET name='a' WHERE owner_id=?; DELETE FROM records",
		"UPDATE records SET name='a' WHERE owner_id=?; 'quoted second statement'",
		"UPDATE records SET name='a' WHERE owner_id=?; /* comment */ 'quoted second statement' /* trailing */",
	} {
		if _, err := (&Compiler{}).CompileStatement(`$dml.Execute("`+sql+`", $Input.OwnerID)`, statementAuthority()); err == nil {
			t.Fatalf("multiple statements admitted: %q", sql)
		}
	}
}
