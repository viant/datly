package fragment

import (
	"context"
	"reflect"
	"testing"

	"github.com/viant/datly/internal/testharness/sqlite"
)

func TestExpressionValuesSQLite(t *testing.T) {
	type row struct{ Matched bool }
	db := sqlite.New(t)
	for _, test := range []struct {
		name       string
		value      any
		expression string
	}{{"zero", 0, "? = 0"}, {"false", false, "? = FALSE"}, {"NULL", nil, "? IS NULL"}, {"typed NULL", (*int)(nil), "? IS NULL"}} {
		t.Run(test.name, func(t *testing.T) {
			bindings := &Bindings{}
			criteria := New(bindings)
			expression, err := criteria.Expression(test.expression, test.value)
			if err != nil {
				t.Fatal(err)
			}
			if !reflect.DeepEqual(bindings.Args(), []any{test.value}) {
				t.Fatalf("args=%#v", bindings.Args())
			}
			db.AssertQuery(t, context.Background(), sqlite.Query{SQL: "SELECT (" + expression + ") AS matched", Args: bindings.Args()}, []row{{true}})
		})
	}
}

func TestContextMissingBindingsFailsExplicitly(t *testing.T) {
	for _, test := range []struct {
		name string
		call func(*Context) (string, error)
	}{
		{"binding", func(c *Context) (string, error) { return c.AppendBinding(1) }},
		{"in", func(c *Context) (string, error) { return c.In("id", []int{1}) }},
		{"like", func(c *Context) (string, error) { return c.Like("name", "one") }},
		{"expression", func(c *Context) (string, error) { return c.Expression("?", false) }},
	} {
		t.Run(test.name, func(t *testing.T) {
			if _, err := test.call(New(nil)); err == nil {
				t.Fatal("missing binding sink accepted")
			}
		})
	}
}
