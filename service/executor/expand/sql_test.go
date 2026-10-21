package expand

import (
	"reflect"
	"testing"
)

func TestStatementsCausalPrefixIncludesRawAndEarlierTables(t *testing.T) {
	statements := NewStmtHolder()
	first := &Executable{Table: "parent", ExecType: ExecTypeInsert}
	raw := &SQLStatment{SQL: "PRAGMA foreign_keys = ON"}
	target := &Executable{Table: "child", ExecType: ExecTypeInsert}
	statements.append(first, nil)
	statements.Execute(raw)
	statements.append(target, nil)
	actual := statements.CausalPrefixByTableName("child")
	want := []interface{}{first, raw, target}
	if !reflect.DeepEqual(actual, want) {
		t.Fatalf("prefix=%v want %v", actual, want)
	}
	first.MarkAsExecuted()
	raw.MarkAsExecuted()
	actual = statements.CausalPrefixByTableName("child")
	if want = []interface{}{target}; !reflect.DeepEqual(actual, want) {
		t.Fatalf("pending prefix=%v want %v", actual, want)
	}
}

func TestStatementsCausalPrefixDoesNotFlushWithoutTarget(t *testing.T) {
	statements := NewStmtHolder()
	statements.Execute(&SQLStatment{SQL: "SELECT 1"})
	if actual := statements.CausalPrefixByTableName("missing"); len(actual) != 0 {
		t.Fatalf("prefix=%v", actual)
	}
}

func TestStatementsEmptyTargetReturnsAllPending(t *testing.T) {
	statements := NewStmtHolder()
	raw := &SQLStatment{SQL: "SELECT 1"}
	mutation := &Executable{Table: "audit", ExecType: ExecTypeInsert}
	statements.Execute(raw)
	statements.append(mutation, nil)
	want := []interface{}{raw, mutation}
	if actual := statements.CausalPrefixByTableName(""); !reflect.DeepEqual(actual, want) {
		t.Fatalf("prefix=%v want %v", actual, want)
	}
}
