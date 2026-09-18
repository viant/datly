package builder

import (
	"context"
	"reflect"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/viant/datly/data"
	"github.com/viant/datly/internal/testharness/sqlite"
	sqltemplate "github.com/viant/datly/sql/template"
	"github.com/viant/sqlx"
	"github.com/viant/sqlx/io/read/cache"
	xstate "github.com/viant/xdatly/state"
)

func TestProjectedPositionalBindings(t *testing.T) {
	ctx := context.Background()
	db := sqlite.New(t)
	require.NoError(t, db.ExecStatements(ctx, "CREATE TABLE projection_values(id INTEGER)", "INSERT INTO projection_values VALUES(1)"))
	for _, mode := range []string{"build", "bound"} {
		for _, tc := range []struct {
			name, source   string
			args, wantArgs []any
			want           int
			selector       *xstate.Selector
		}{
			{"drop first", "SELECT ? AS discard, ? AS result FROM projection_values WHERE id=?", []any{99, 7, 1}, []any{7, 1}, 7, nil},
			{"drop middle and last", "SELECT ? AS discard, ? AS result, ? AS other FROM projection_values WHERE id=?", []any{99, 7, 88, 1}, []any{7, 1}, 7, nil},
			{"drop all select bindings", "SELECT ? AS discard, id AS result FROM projection_values WHERE id=?", []any{99, 1}, []any{1}, 1, nil},
			{"multiple in expression", "SELECT COALESCE(?, ?) AS discard, ? + ? AS result FROM projection_values WHERE id=?", []any{99, 88, 3, 4, 1}, []any{3, 4, 1}, 7, nil},
			{"named collision", "SELECT ? AS discard, :__datly_projection_arg_0 + ? AS result FROM projection_values WHERE id=?", []any{99, 3, 1}, []any{4, 3, 1}, 7, nil},
			{"CTE prefix", "WITH p AS (SELECT ? AS id) SELECT ? AS discard, id AS result FROM p WHERE id=?", []any{1, 99, 1}, []any{1, 1}, 1, nil},
			{"nested source", "SELECT ? AS discard, id AS result FROM (SELECT ? AS id) p WHERE id=?", []any{99, 1, 1}, []any{1, 1}, 1, nil},
			{"union preserves inner bindings", "SELECT ? AS discard, ? AS result FROM projection_values UNION SELECT ? AS discard, ? AS result FROM projection_values", []any{99, 7, 99, 7}, []any{99, 7, 99, 7}, 7, nil},
			{"explicit selector slot", "SELECT ? AS discard, ? AS result FROM projection_values WHERE id=? $AND_SELECTOR_CRITERIA AND id<?", []any{99, 7, 1, 2}, []any{7, 1, 0, 2}, 7, &xstate.Selector{Criteria: "id > ?", Placeholders: []any{0}}},
			{"slice value", "SELECT ? AS discard, id AS result FROM projection_values WHERE id IN (?)", []any{99, []int{1, 2}}, []any{1, 2}, 1, nil},
			{"nil value", "SELECT ? AS discard, COALESCE(?, 7) AS result FROM projection_values", []any{99, nil}, []any{nil}, 7, nil},
		} {
			t.Run(mode+"/"+tc.name, func(t *testing.T) {
				opts := []BuilderOption{WithBuilderSQL(tc.source), WithBuilderProjection([]string{"result"}), WithBuilderSelector(tc.selector), WithBuilderParameterResolver(parameterResolver(map[string]any{"__datly_projection_arg_0": 4}))}
				if tc.selector != nil {
					opts = append(opts, WithBuilderView(&data.View{Columns: []*data.Column{{Name: "id"}}}))
				}
				var q *cache.ParmetrizedQuery
				var err error
				if mode == "bound" {
					q, err = NewBuilder().ShapeBound(&cache.ParmetrizedQuery{SQL: tc.source, Args: tc.args}, opts...)
				} else {
					q, err = NewBuilder().Build(ctx, append(opts, WithBuilderPositionalArgs(tc.args))...)
				}
				require.NoError(t, err)
				require.Equal(t, tc.wantArgs, q.Args, q.SQL)
				require.NotContains(t, q.SQL, "__datly_projection_arg_")
				var result int
				require.NoError(t, db.DB.QueryRowContext(ctx, q.SQL, q.Args...).Scan(&result), q.SQL)
				require.Equal(t, tc.want, result)
			})
		}
	}
}

func TestProjectionBindingsTemplateAndCache(t *testing.T) {
	type input struct{ Publisher, Advertiser int }
	evaluator, err := (sqltemplate.Compiler{
		Source:    `SELECT $criteria.AppendBinding($Unsafe.Publisher) AS publisher_id, c.id FROM creatives c LEFT JOIN approvals a ON a.id=c.id AND a.publisher=$criteria.AppendBinding($Unsafe.Publisher) WHERE c.advertiser=$criteria.AppendBinding($Unsafe.Advertiser)`,
		InputType: reflect.TypeFor[input](),
	}).Compile()
	require.NoError(t, err)
	opts := []BuilderOption{WithBuilderTemplate(evaluator), WithBuilderInput(reflect.ValueOf(input{8, 123})), WithBuilderProjection([]string{"id"})}
	for _, build := range []func(context.Context, ...BuilderOption) (*cache.ParmetrizedQuery, error){NewBuilder().Build, NewBuilder().CacheSQL} {
		q, err := build(context.Background(), opts...)
		require.NoError(t, err)
		require.Equal(t, []any{8, 123}, q.Args)
		require.NotContains(t, q.SQL, "AS publisher_id")
		require.NotContains(t, q.SQL, "__datly_projection_arg_")
	}
}

func TestProjectionBindingsValidateOriginalArguments(t *testing.T) {
	for _, args := range [][]any{nil, {1, 2, 3}} {
		_, err := NewBuilder().Build(context.Background(), WithBuilderSQL("SELECT ? AS discard, ? AS other, 1 AS result FROM records"), WithBuilderProjection([]string{"result"}), WithBuilderPositionalArgs(args))
		if len(args) == 0 {
			require.ErrorContains(t, err, "missing positional SQL argument")
		} else {
			require.ErrorContains(t, err, "positional arguments but 3 were supplied")
		}
	}
}

func TestGroupedProjectionRemovesBindings(t *testing.T) {
	ctx := context.Background()
	db := sqlite.New(t)
	require.NoError(t, db.ExecStatements(ctx, "CREATE TABLE projection_sales(id INTEGER, amount INTEGER)", "INSERT INTO projection_sales VALUES(1,6),(1,7)"))
	source := "SELECT id + ? AS bucket, SUM(amount + ?) AS total, ? AS discard FROM projection_sales WHERE id > ? GROUP BY id + ?"
	for _, mode := range []string{"build", "bound"} {
		t.Run(mode, func(t *testing.T) {
			opts := []BuilderOption{WithBuilderSQL(source), WithBuilderView(resolvedGroupableView()), WithBuilderProjection([]string{"bucket", "total"})}
			var q *cache.ParmetrizedQuery
			var err error
			if mode == "bound" {
				q, err = NewBuilder().ShapeBound(&cache.ParmetrizedQuery{SQL: source, Args: []any{10, 2, 99, 0, 10}}, opts...)
			} else {
				q, err = NewBuilder().Build(ctx, append(opts, WithBuilderPositionalArgs([]any{10, 2, 99, 0, 10}))...)
			}
			require.NoError(t, err)
			require.Equal(t, []any{10, 2, 0}, q.Args, q.SQL)
			var bucket, total int
			require.NoError(t, db.DB.QueryRowContext(ctx, q.SQL, q.Args...).Scan(&bucket, &total), q.SQL)
			require.Equal(t, 11, bucket)
			require.Equal(t, 17, total)
		})
	}
}

func TestProjectionBindingIdentitySurvivesReorderingAndRepetition(t *testing.T) {
	source, resolver, err := nameProjectionBindings("SELECT ? AS first, ? AS second", []any{11, 22}, nil)
	require.NoError(t, err)
	require.Contains(t, source, ":__datly_projection_arg_0")
	binder := sqlx.NewParameterBinder(resolver)
	sql, args, err := binder.Bind("SELECT :__datly_projection_arg_1, :__datly_projection_arg_0, :__datly_projection_arg_1")
	require.NoError(t, err)
	require.NoError(t, binder.Complete())
	require.Equal(t, "SELECT ?, ?, ?", sql)
	require.Equal(t, []any{22, 11, 22}, args)
}
