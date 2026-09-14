package sqlite

import (
	"context"
	"reflect"
	"testing"
)

func TestTypedQueryVerification(t *testing.T) {
	type row struct {
		ID   int     `sqlx:"id"`
		Name *string `sqlx:"name"`
	}
	name := "Ada"
	for _, tc := range []struct {
		name     string
		query    Query
		expected any
	}{
		{"value rows", Query{SQL: "SELECT id,name FROM records ORDER BY id"}, []row{{1, &name}, {2, nil}}},
		{"pointer rows", Query{SQL: "SELECT id,name FROM records WHERE id=?", Args: []any{2}}, []*row{{ID: 2}}},
		{"empty rows", Query{SQL: "SELECT id,name FROM records WHERE id=?", Args: []any{3}}, []row(nil)},
	} {
		t.Run(tc.name, func(t *testing.T) {
			h := New(t)
			ctx := context.Background()
			if err := h.ExecStatements(ctx, "CREATE TABLE records(id INTEGER, name TEXT)", "INSERT INTO records VALUES(1,'Ada'),(2,NULL)"); err != nil {
				t.Fatal(err)
			}
			h.AssertQuery(t, ctx, tc.query, tc.expected)
		})
	}
}

func TestReadQueryRejectsInvalidContractsAndSQL(t *testing.T) {
	h := New(t)
	for _, shape := range []reflect.Type{nil, reflect.TypeFor[int](), reflect.TypeFor[[]int](), reflect.TypeFor[[]**struct{}]()} {
		if _, err := h.ReadQuery(context.Background(), Query{SQL: "SELECT 1"}, shape); err == nil {
			t.Fatalf("accepted %v", shape)
		}
	}
	if _, err := h.ReadQuery(context.Background(), Query{SQL: "SELECT missing FROM missing"}, reflect.TypeFor[[]struct{ ID int }]()); err == nil {
		t.Fatal("missing SQL error")
	}
}
