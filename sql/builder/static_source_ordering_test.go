package builder

import (
	"github.com/stretchr/testify/require"
	"github.com/viant/datly/data"
	"github.com/viant/datly/internal/testharness/sqlite"
	"github.com/viant/datly/spec"
	dsql "github.com/viant/datly/sql"
	sqltemplate "github.com/viant/datly/sql/template"
	xstate "github.com/viant/xdatly/state"
	"reflect"
	"testing"
)

func TestStaticCompanionPreservesAuthorizedSourceOrdering(t *testing.T) {
	const inner = "SELECT DISTINCT p.label AS value FROM items p WHERE p.tenant_id=7"
	const source = "SELECT result.* FROM (" + inner + ") result"
	h := sqlite.New(t)
	require.NoError(t, h.ExecStatements(t.Context(), "CREATE TABLE items(id INTEGER,tenant_id INTEGER,label TEXT)", "INSERT INTO items VALUES(1,7,'alpha'),(2,7,'beta'),(3,7,'alpha'),(99,8,'outside')"))
	for _, fields := range [][]string{nil, {"value"}} {
		for _, order := range []string{"", "id", "p.id:desc"} {
			t.Run(order, func(t *testing.T) {
				view := &data.View{Spec: spec.View{Source: &spec.ViewSource{SQL: source}}, Columns: []*data.Column{{Name: "value", Column: "value"}}}
				plan, err := dsql.NewStaticProjection(source, view, nil)
				require.NoError(t, err)
				selection, err := plan.Select(fields)
				require.NoError(t, err)
				evaluator, err := (sqltemplate.Compiler{Source: source, InputType: reflect.TypeFor[struct{}]()}).Compile()
				require.NoError(t, err)
				policy := &spec.Selector{AllowFields: true, AllowOrderBy: true, DefaultOrder: "p.id DESC", Orderable: []spec.FieldPath{"p.id"}}
				q, err := NewBuilder().Build(t.Context(), WithBuilderView(view), WithBuilderTemplate(evaluator), WithBuilderInput(reflect.ValueOf(struct{}{})), WithBuilderProjection(fields), WithBuilderStaticProjection(selection, evaluator), WithBuilderSelectorPolicy(policy), WithBuilderSelector(&xstate.Selector{OrderBy: order}))
				require.NoError(t, err)
				require.Contains(t, q.SQL, "SELECT DISTINCT")
				require.Contains(t, q.SQL, "ORDER BY p.id")
				require.NotContains(t, q.SQL, "p.id AS")
				rows, err := h.DB.QueryContext(t.Context(), q.SQL)
				require.NoError(t, err)
				defer rows.Close()
				columns, err := rows.Columns()
				require.NoError(t, err)
				require.Equal(t, []string{"value"}, columns)
				var values []string
				for rows.Next() {
					var value string
					require.NoError(t, rows.Scan(&value))
					values = append(values, value)
				}
				require.NoError(t, rows.Err())
				require.ElementsMatch(t, []string{"alpha", "beta"}, values)
			})
		}
	}
}
