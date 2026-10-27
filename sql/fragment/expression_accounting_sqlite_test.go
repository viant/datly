package fragment

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/viant/datly/internal/testharness/sqlite"
	"github.com/viant/sqlx"
)

func TestExpressionPlaceholderAccountingSQLite(t *testing.T) {
	type row struct {
		Matched bool `sqlx:"matched"`
	}
	h := sqlite.New(t)
	for _, tc := range []struct {
		name, expression string
		value            any
		slots            int
		matched          bool
	}{
		{"constant true", "1 = 1", 123, 0, true},
		{"constant false", "1 = 0", 123, 0, false},
		{"quoted question", "'?' = '?'", 123, 0, true},
		{"comment question", "1 /* ? */ = 1 -- ?\n", 123, 0, true},
		{"positional", "? = 7", 7, 1, true},
		{"positional with protected question", "? = '?' /* ? */", "?", 1, true},
		{"named", ":bound = 7", 999, 0, true},
		{"mixed named and positional", "? = :bound", 7, 1, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			bindings := NewBindings()
			expression, err := New(bindings).Expression(tc.expression, tc.value)
			require.NoError(t, err)
			require.Equal(t, tc.expression, expression)
			require.Len(t, bindings.Args(), tc.slots, "only emitted positional slots consume FilterValue")
			binder := sqlx.NewParameterBinder(func(name string) (any, bool, error) { return 7, name == "bound", nil }, bindings.Args()...)
			SQL, args, err := binder.Bind("SELECT (" + expression + ") AS matched")
			require.NoError(t, err)
			require.NoError(t, binder.Complete())
			h.AssertQuery(t, context.Background(), sqlite.Query{SQL: SQL, Args: args}, []row{{tc.matched}})
		})
	}
}
