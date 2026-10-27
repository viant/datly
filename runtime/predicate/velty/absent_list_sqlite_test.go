package velty

import (
	"context"
	"reflect"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/viant/datly/internal/testharness/sqlite"
	"github.com/viant/datly/spec"
	"github.com/viant/sqlx"
)

func TestApplyWhenAbsentListsSQLite(t *testing.T) {
	type input struct {
		IDs   []int
		Kinds []string
	}
	type row struct {
		ID int `sqlx:"id"`
	}
	h := sqlite.New(t)
	ctx := context.Background()
	require.NoError(t, h.ExecStatements(ctx, "CREATE TABLE selected_records(id INTEGER,kind TEXT)", "INSERT INTO selected_records VALUES(1,'a'),(2,'b'),(3,'a'),(4,'c')"))
	component := &spec.Component{Parameters: []*spec.Parameter{
		{Name: "IDs", Predicates: []*spec.Predicate{{Group: 0, Name: "in", Args: []string{"r", "id"}, ApplyWhenAbsent: true}}},
		{Name: "Kinds", Predicates: []*spec.Predicate{{Group: 1, Name: "in", Args: []string{"r", "kind"}, ApplyWhenAbsent: true}}},
	}}
	program, err := Compile(CompileInput{Component: component, InputType: reflect.TypeFor[input]()})
	require.NoError(t, err)
	for _, tc := range []struct {
		name  string
		value input
		want  []row
		count int
	}{
		{"present", input{[]int{1, 2}, []string{"a", "b"}}, []row{{1}, {2}}, 4},
		{"empty", input{[]int{}, []string{}}, []row{}, 0},
		{"absent", input{}, []row{}, 0},
		{"ids-only", input{IDs: []int{1, 2}}, []row{}, 2},
		{"kinds-only", input{Kinds: []string{"a", "b"}}, []row{}, 2},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var args []any
			bound, err := program.NewContext(ctx, predicateBinder{input: &tc.value}, func(values ...any) { args = append(args, values...) })
			require.NoError(t, err)
			first, err := bound.(*Context).FilterGroup(0, "AND")
			require.NoError(t, err)
			second, err := bound.(*Context).FilterGroup(1, "AND")
			require.NoError(t, err)
			SQL := "SELECT r.id FROM selected_records r WHERE (" + first + ") AND (" + second + ") ORDER BY r.id"
			require.Len(t, args, tc.count)
			require.Equal(t, len(args), sqlx.ParseParameters(SQL).PositionalCount(), "every collected value must have an emitted slot")
			if len(tc.value.IDs) == 0 {
				require.Contains(t, first, "1 = 0")
			}
			if len(tc.value.Kinds) == 0 {
				require.Contains(t, second, "1 = 0")
			}
			h.AssertQuery(t, ctx, sqlite.Query{SQL: SQL, Args: args}, tc.want)
		})
	}
}
