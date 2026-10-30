package velty

import (
	"context"
	"encoding/json"
	"reflect"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/viant/datly/internal/testharness/sqlite"
	"github.com/viant/datly/spec"
)

func TestNullableInNativePredicateSQLite(t *testing.T) {
	type input struct {
		Segments []*string
		Has      *struct{ Segments bool } `setMarker:"true"`
	}
	type row struct {
		ID int `sqlx:"id"`
	}
	h := sqlite.New(t)
	ctx := context.Background()
	require.NoError(t, h.ExecStatements(ctx, "CREATE TABLE records(id INTEGER, tenant INTEGER, segment TEXT)", "INSERT INTO records VALUES (1,7,NULL),(2,7,'Cedar'),(3,7,'null'),(4,8,NULL)"))
	for _, predicate := range []struct {
		name string
		args []string
	}{
		{"nullable_in", []string{"r", "segment"}},
		{"nullable_literal_in", []string{"CASE WHEN r.segment IS NULL THEN NULL ELSE r.segment || '' END"}},
	} {
		t.Run(predicate.name, func(t *testing.T) {
			component := &spec.Component{Parameters: []*spec.Parameter{{Name: "Segments", Predicates: []*spec.Predicate{{Name: predicate.name, Args: predicate.args}}}}}
			program, err := Compile(CompileInput{Component: component, InputType: reflect.TypeFor[input]()})
			require.NoError(t, err)
			for _, tc := range []struct {
				wire string
				want []row
				args []any
			}{
				{`{}`, []row{{1}, {2}, {3}}, nil},
				{`{"Segments":null}`, []row{{1}, {2}, {3}}, nil},
				{`{"Segments":[],"Has":{"Segments":true}}`, []row{{1}, {2}, {3}}, nil},
				{`{"Segments":[null],"Has":{"Segments":true}}`, []row{{1}}, nil},
				{`{"Segments":[null,"Cedar"],"Has":{"Segments":true}}`, []row{{1}, {2}}, []any{"Cedar"}},
				{`{"Segments":["null"],"Has":{"Segments":true}}`, []row{{3}}, []any{"null"}},
			} {
				var value input
				require.NoError(t, json.Unmarshal([]byte(tc.wire), &value))
				var args []any
				bound, err := program.NewContext(ctx, predicateBinder{input: &value}, func(values ...any) { args = append(args, values...) })
				require.NoError(t, err)
				clause, err := bound.(*Context).FilterGroup(0, "AND")
				require.NoError(t, err)
				require.Equal(t, tc.args, args)
				query := "SELECT r.id FROM records r WHERE r.tenant=7"
				if clause != "" {
					query += " AND " + clause
				}
				query += " ORDER BY r.id"
				h.AssertQuery(t, ctx, sqlite.Query{SQL: query, Args: args}, tc.want)
			}
			for _, typ := range []reflect.Type{reflect.TypeFor[[]string](), reflect.TypeFor[string](), reflect.TypeFor[[]any]()} {
				_, err = Compile(CompileInput{Component: component, InputType: reflect.StructOf([]reflect.StructField{{Name: "Segments", Type: typ}})})
				require.ErrorContains(t, err, "pointer-element slice")
			}
		})
	}
}
