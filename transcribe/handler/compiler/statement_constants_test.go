package compiler

import (
	"github.com/viant/datly/constant"
	"reflect"
	"testing"
)

func TestBufferedStatementImmutableIdentifierConstants(t *testing.T) {
	values, err := constant.New(map[string]string{"Table": "records"})
	if err != nil {
		t.Fatal(err)
	}
	const sql = "UPDATE `${Table}` SET name = '?' /* $ignored */ WHERE owner_id = ?"
	source := `$dml.Execute("` + sql + `", $Input.OwnerID)`
	result, err := (&Compiler{}).CompileStatement(source, statementAuthority(), values)
	if err != nil {
		t.Fatal(err)
	}
	if result.SQL != sql || !reflect.DeepEqual(result.SQLConstants, []string{"Table"}) || len(result.Arguments) != 1 {
		t.Fatalf("authored SQL or binding changed: %+v", result)
	}
	for _, bad := range []string{
		`$dml.Execute("UPDATE $Input.Name SET name = ?", $Input.Name)`,
		`$dml.Execute("UPDATE ` + "`${Unknown}`" + ` SET name = ?", $Input.Name)`,
		`$dml.Execute("UPDATE ` + "`${Table}`" + ` SET name = $Input.Name")`,
	} {
		if _, err := (&Compiler{}).CompileStatement(bad, statementAuthority(), values); err == nil {
			t.Fatalf("accepted request SQL: %s", bad)
		}
	}
	invalid, _ := constant.New(map[string]string{"Table": "records; DELETE FROM audit"})
	if _, err := (&Compiler{}).CompileStatement(source, statementAuthority(), invalid); err == nil {
		t.Fatal("accepted SQL syntax in identifier constant")
	}
}
