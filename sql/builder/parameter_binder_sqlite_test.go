package builder_test

import (
	"context"
	"github.com/viant/datly/internal/testharness/sqlite"
	"github.com/viant/sqlx"
	"testing"
)

func TestParameterBinderSQLite(t *testing.T) {
	type row struct {
		ID int `sqlx:"id"`
	}
	for _, tc := range []struct {
		name, SQL string
		named     map[string]any
		args      []any
		want      []row
	}{
		{"named", "SELECT id FROM records WHERE tenant=:tenant ORDER BY id", map[string]any{"tenant": "a"}, nil, []row{{1}, {2}}},
		{"mixed", "SELECT id FROM records WHERE tenant=:tenant AND id>? ORDER BY id", map[string]any{"tenant": "a"}, []any{1}, []row{{2}}},
		{"slice", "SELECT id FROM records WHERE id IN (:ids) ORDER BY id", map[string]any{"ids": []int{1, 3}}, nil, []row{{1}, {3}}},
		{"empty_slice", "SELECT id FROM records WHERE id IN (:ids)", map[string]any{"ids": []int{}}, nil, []row{}},
		{"quoted", "SELECT id FROM records WHERE tenant=:tenant AND ':ignored ?'=':ignored ?' /* ? */ ORDER BY id", map[string]any{"tenant": "b"}, nil, []row{{3}}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx := context.Background()
			db := sqlite.New(t)
			if err := db.ExecStatements(ctx, "CREATE TABLE records(id INTEGER,tenant TEXT)", "INSERT INTO records VALUES(1,'a'),(2,'a'),(3,'b')"); err != nil {
				t.Fatal(err)
			}
			binder := sqlx.NewParameterBinder(func(name string) (any, bool, error) { value, ok := tc.named[name]; return value, ok, nil }, tc.args...)
			SQL, args, err := binder.Bind(tc.SQL)
			if err != nil {
				t.Fatal(err)
			}
			if err := binder.Complete(); err != nil {
				t.Fatal(err)
			}
			db.AssertQuery(t, ctx, sqlite.Query{SQL: SQL, Args: args}, tc.want)
		})
	}
}
