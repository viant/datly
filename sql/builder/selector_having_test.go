package builder

import (
	"context"
	"reflect"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/viant/datly/internal/testharness/sqlite"
	"github.com/viant/datly/spec"
	"github.com/viant/datly/sql/criteria"
	"github.com/viant/sqlx/io/read/cache"
	xstate "github.com/viant/xdatly/state"
)

func TestAggregateSelectorCriteriaSQLite(t *testing.T) {
	ctx := context.Background()
	db := sqlite.New(t)
	require.NoError(t, db.ExecStatements(ctx,
		"CREATE TABLE sales(tenant INTEGER, amount INTEGER)",
		"INSERT INTO sales VALUES(1,6),(1,6),(2,9),(3,20)"))
	type row struct{ Tenant, Amount int }
	for _, mode := range []string{"build", "bound", "cache"} {
		for _, tc := range []struct {
			name, predicate string
			want            []row
			args            []any
		}{
			{"alias collision", "amount > 10", []row{{1, 12}, {3, 20}}, []any{int64(10)}},
			{"unselected aggregate", "items > 1", []row{{1, 12}}, []any{int64(1)}},
			{"mixed or", "items > 1 OR tenant = 2", []row{{1, 12}, {2, 9}}, []any{int64(1), int64(2)}},
			{"mixed and", "items > 1 AND tenant = 1", []row{{1, 12}}, []any{int64(1), int64(1)}},
			{"aggregate on rhs", "tenant < items", []row{{1, 12}}, nil},
		} {
			t.Run(mode+"/"+tc.name, func(t *testing.T) {
				source := "SELECT tenant, SUM(amount) AS amount, COUNT(*) AS items FROM sales WHERE tenant > ? GROUP BY tenant HAVING SUM(amount) > ? OR COUNT(*) > ? ORDER BY tenant"
				selector := &xstate.Selector{Criteria: tc.predicate}
				opts := []BuilderOption{WithBuilderSQL(source), WithBuilderView(resolvedGroupableView()), WithBuilderProjection([]string{"tenant", "amount"}), WithBuilderSelector(selector)}
				authored := []any{0, 10, 0}
				var query *cache.ParmetrizedQuery
				var err error
				switch mode {
				case "bound":
					query, err = NewBuilder().ShapeBound(&cache.ParmetrizedQuery{SQL: source, Args: authored}, opts...)
				case "cache":
					named := strings.Replace(source, "tenant > ?", "tenant > :Tenant", 1)
					named = strings.Replace(named, "SUM(amount) > ?", "SUM(amount) > :Amount", 1)
					named = strings.Replace(named, "COUNT(*) > ?", "COUNT(*) > :Items", 1)
					query, err = NewBuilder().CacheSQL(ctx, append(opts, WithBuilderSQL(named), WithBuilderParameterResolver(parameterResolver(map[string]any{"tenant": 0, "amount": 10, "items": 0})))...)
				default:
					query, err = NewBuilder().Build(ctx, append(opts, WithBuilderPositionalArgs(authored))...)
				}
				require.NoError(t, err)
				require.Equal(t, tc.predicate, selector.Criteria)
				require.Equal(t, append(authored, tc.args...), query.Args)
				require.Contains(t, query.SQL, "HAVING (SUM(amount) > ? OR COUNT(*) > ?) AND")
				require.NotContains(t, query.SQL, "AS items")
				require.NotContains(t, strings.Split(query.SQL, "GROUP BY")[0], "amount > ?")
				db.AssertQuery(t, ctx, sqlite.Query{SQL: query.SQL, Args: query.Args}, tc.want)
			})
		}
	}
}

func TestAggregateSelectorCriteriaValidation(t *testing.T) {
	for _, tc := range []struct {
		name, source, predicate string
		reject                  bool
	}{
		{"global aggregate", "SELECT SUM(amount) AS total FROM sales", "total > 1", false},
		{"no authored having", "SELECT tenant, SUM(amount) AS total FROM sales GROUP BY tenant ORDER BY tenant LIMIT 2", "total > 1", false},
		{"typed alias", "SELECT SUM(amount) AS total FROM sales", "Total > 'bad'", true},
		{"parameterized aggregate", "SELECT SUM(amount + ?) AS total FROM sales", "total > 1", true},
		{"explicit placement", "SELECT SUM(amount) AS total FROM sales $WHERE_SELECTOR_CRITERIA", "total > 1", true},
		{"set operation", "SELECT SUM(amount) AS total FROM sales UNION SELECT SUM(amount) AS total FROM sales", "total > 1", true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			query, err := NewBuilder().Build(context.Background(), WithBuilderSQL(tc.source), WithBuilderSelector(&xstate.Selector{Criteria: tc.predicate}), WithBuilderCriteriaCompiler(&criteria.Compiler{Columns: map[string]criteria.Column{"Total": {Expression: "total", Type: reflect.TypeFor[int]()}}}))
			if tc.reject {
				require.Error(t, err)
				return
			}
			require.NoError(t, err)
			require.Contains(t, query.SQL, "HAVING ((SUM(amount)) > ?)")
		})
	}
	_, err := NewBuilder().Build(context.Background(), WithBuilderSQL("SELECT SUM(amount) AS total FROM sales"), WithBuilderSelector(&xstate.Selector{Criteria: "total > 1"}), WithBuilderSelectorPolicy(&spec.Selector{AllowCriteria: true}))
	require.Error(t, err, "aggregate discovery must not bypass the filterable allowlist")
}
