package criteria

import (
	"context"
	"reflect"
	"testing"

	"github.com/viant/datly/internal/testharness/sqlite"
)

func TestCompilerSQLite(t *testing.T) {
	h := sqlite.New(t)
	if err := h.ExecStatements(context.Background(), "CREATE TABLE records(id INTEGER, tenant INTEGER, name TEXT, active BOOLEAN, amount REAL)", "INSERT INTO records VALUES (1,7,'alice',true,1.5),(2,7,'bob',false,2.5),(3,8,'alice',true,3.5)"); err != nil {
		t.Fatal(err)
	}
	c := Compiler{Columns: map[string]Column{"id": {Expression: "id"}, "name": {Expression: "name"}, "active": {Expression: "active"}, "amount": {Expression: "amount"}}}
	for _, tt := range []struct {
		name, source string
		args         []any
		ids          []int
		wantArgs     []any
		reject       bool
	}{
		{name: "integer", source: "id = 1", ids: []int{1}, wantArgs: []any{int64(1)}},
		{name: "string", source: "name = 'alice'", ids: []int{1}, wantArgs: []any{"alice"}},
		{name: "boolean", source: "active = true", ids: []int{1}, wantArgs: []any{true}},
		{name: "numeric", source: "amount > 2.0", ids: []int{2}, wantArgs: []any{float64(2)}},
		{name: "or scope", source: "id = 2 OR name = 'alice'", ids: []int{1, 2}, wantArgs: []any{int64(2), "alice"}},
		{name: "nested", source: "(id = 2 OR id = 3) AND name = 'bob'", ids: []int{2}, wantArgs: []any{int64(2), int64(3), "bob"}},
		{name: "in", source: "id IN (1,2)", ids: []int{1, 2}, wantArgs: []any{int64(1), int64(2)}},
		{name: "placeholder", source: "id = ?", args: []any{2}, ids: []int{2}, wantArgs: []any{2}},
		{name: "literal payload", source: "name = 'alice OR 1=1'", wantArgs: []any{"alice OR 1=1"}},
		{name: "unknown", source: "secret = 1", reject: true},
		{name: "nonfilterable", source: "tenant = 8", reject: true},
		{name: "constant injection", source: "id = 1 OR 1=1", reject: true},
		{name: "statement", source: "id = 1; DELETE FROM records", reject: true},
		{name: "comment", source: "id = 1 -- hi", reject: true},
		{name: "subquery", source: "id IN (SELECT id FROM records)", reject: true},
		{name: "function", source: "id = random()", reject: true},
		{name: "missing argument", source: "id = ?", reject: true},
		{name: "unused argument", source: "id = 1", args: []any{2}, reject: true},
		{name: "nested trailing text", source: "(id = 1 garbage)", reject: true},
		{name: "list trailing text", source: "id IN (1,2 garbage)", reject: true},
		{name: "list trailing comma", source: "id IN (1,)", reject: true},
		{name: "mixed precedence", source: "id = 1 AND name = 'bob' OR id = 2", ids: []int{2}, wantArgs: []any{int64(1), "bob", int64(2)}},
		{name: "escaped quote", source: "name = 'O''Brien'", wantArgs: []any{"O'Brien"}},
	} {
		t.Run(tt.name, func(t *testing.T) {
			sql, args, err := c.Compile(tt.source, tt.args)
			if tt.reject {
				if err == nil {
					t.Fatalf("expected rejection: %s %#v", sql, args)
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			if !reflect.DeepEqual(args, tt.wantArgs) {
				t.Fatalf("args %#v, expected %#v", args, tt.wantArgs)
			}
			type row struct {
				ID int `sqlx:"id"`
			}
			var expected []row
			for _, id := range tt.ids {
				expected = append(expected, row{ID: id})
			}
			h.AssertQuery(t, context.Background(), sqlite.Query{SQL: "SELECT id FROM records WHERE tenant=7 AND " + sql + " ORDER BY id", Args: args}, expected)
		})
	}
}
