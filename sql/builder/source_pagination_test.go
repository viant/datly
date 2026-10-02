package builder

import (
	"context"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/viant/datly/data"
	"github.com/viant/datly/internal/testharness/sqlite"
	"github.com/viant/datly/spec"
	"github.com/viant/sqlx/io/read/cache"
	xstate "github.com/viant/xdatly/state"
)

func sourcePaginationView() *data.View {
	return &data.View{Columns: []*data.Column{{Name: "ID", Column: "id"}, {Name: "Name", Column: "name"}}}
}

func TestTypedSourcePaginationBuildAndShapeBoundSQLite(t *testing.T) {
	ctx := context.Background()
	db := sqlite.New(t)
	require.NoError(t, db.ExecStatements(ctx, "CREATE TABLE users(id INTEGER,name TEXT)", "INSERT INTO users VALUES(1,'z'),(2,'y'),(3,'x'),(4,'w'),(5,'v'),(6,'u')"))
	const source = "SELECT t.* FROM (SELECT id,name FROM users ORDER BY id $PAGINATION) t"
	for _, mode := range []string{"Build", "ShapeBound"} {
		for _, selection := range []string{"default", "selected", "selector order", "selected selector order"} {
			t.Run(mode+"/"+selection, func(t *testing.T) {
				limit, offset := 2, 2
				controls := &spec.ViewControls{Limit: &limit, Offset: &offset}
				opts := []BuilderOption{WithBuilderView(sourcePaginationView()), WithBuilderSQL(source), WithBuilderControls(controls)}
				selected := strings.Contains(selection, "selected")
				ordered := strings.Contains(selection, "order")
				if selected {
					opts = append(opts, WithBuilderProjection([]string{"name"}))
				}
				if ordered {
					opts = append(opts, WithBuilderSelector(&xstate.Selector{OrderBy: "name ASC"}))
				}
				var q *cache.ParmetrizedQuery
				var err error
				if mode == "Build" {
					q, err = NewBuilder().Build(ctx, opts...)
				} else {
					q, err = NewBuilder().ShapeBound(&cache.ParmetrizedQuery{SQL: source}, opts...)
				}
				require.NoError(t, err)
				require.Equal(t, 1, strings.Count(strings.ToUpper(q.SQL), "LIMIT"), q.SQL)
				require.Equal(t, 1, strings.Count(strings.ToUpper(q.SQL), "OFFSET"), q.SQL)
				require.NotContains(t, q.SQL, "$PAGINATION")
				require.Equal(t, 2, *controls.Limit)
				require.Equal(t, 2, *controls.Offset)
				require.Empty(t, controls.OrderBy)
				if mode == "ShapeBound" {
					require.Equal(t, 2, q.Limit)
					require.Equal(t, 2, q.Offset)
				}
				rows, err := db.DB.QueryContext(ctx, q.SQL, q.Args...)
				require.NoError(t, err, q.SQL)
				defer rows.Close()
				var names []string
				for rows.Next() {
					var name string
					var id int
					if selected {
						err = rows.Scan(&name)
					} else {
						err = rows.Scan(&id, &name)
					}
					require.NoError(t, err)
					names = append(names, name)
				}
				require.NoError(t, rows.Err())
				want := []string{"x", "w"}
				if ordered {
					want = []string{"w", "x"}
				}
				require.Equal(t, want, names, q.SQL)
			})
		}
	}
}

func TestSourcePaginationPolicyWindow(t *testing.T) {
	const source = "SELECT t.* FROM (SELECT id,name FROM users ORDER BY id $PAGINATION) t"
	for _, mode := range []string{"Build", "ShapeBound"} {
		for _, tc := range []struct {
			name       string
			policy     *spec.Selector
			selector   *xstate.Selector
			exclude    bool
			want, deny string
		}{
			{name: "capped page", policy: &spec.Selector{AllowLimit: true, AllowPage: true, DefaultLimit: 2}, selector: &xstate.Selector{Limit: 9, Page: 2}, want: "LIMIT 2 OFFSET 2"},
			{name: "default limit", policy: &spec.Selector{DefaultLimit: 2}, want: "LIMIT 2"},
			{name: "unlimited page", policy: &spec.Selector{NoLimit: true, AllowPage: true}, selector: &xstate.Selector{Page: 2}},
			{name: "excluded summary window", policy: &spec.Selector{DefaultLimit: 2}, selector: &xstate.Selector{Limit: 9, Offset: 2}, exclude: true},
			{name: "denied limit", policy: &spec.Selector{}, selector: &xstate.Selector{Limit: 2}, deny: "limit is not allowed"},
			{name: "denied offset", policy: &spec.Selector{AllowLimit: true}, selector: &xstate.Selector{Limit: 2, Offset: 1}, deny: "offset is not allowed"},
			{name: "denied page", policy: &spec.Selector{DefaultLimit: 2}, selector: &xstate.Selector{Page: 2}, deny: "page is not allowed"},
			{name: "denied order", policy: &spec.Selector{}, selector: &xstate.Selector{OrderBy: "name"}, deny: "order by is not allowed"},
		} {
			t.Run(mode+"/"+tc.name, func(t *testing.T) {
				opts := []BuilderOption{WithBuilderSQL(source), WithBuilderView(sourcePaginationView()), WithBuilderSelectorPolicy(tc.policy), WithBuilderSelector(tc.selector), WithBuilderExcludePagination(tc.exclude)}
				var q *cache.ParmetrizedQuery
				var err error
				if mode == "Build" {
					q, err = NewBuilder().Build(context.Background(), opts...)
				} else {
					q, err = NewBuilder().ShapeBound(&cache.ParmetrizedQuery{SQL: source}, opts...)
				}
				if tc.deny != "" {
					require.ErrorContains(t, err, tc.deny)
					return
				}
				require.NoError(t, err)
				require.NotContains(t, q.SQL, "$PAGINATION")
				if tc.want != "" {
					require.Contains(t, q.SQL, tc.want)
					require.Equal(t, 1, strings.Count(strings.ToUpper(q.SQL), "LIMIT"))
				} else {
					require.NotContains(t, strings.ToUpper(q.SQL), "LIMIT")
					require.NotContains(t, strings.ToUpper(q.SQL), "OFFSET")
				}
			})
		}
	}
	limit, offset := 2, 2
	controls := &spec.ViewControls{Limit: &limit, Offset: &offset, OrderBy: "name DESC"}
	q, err := NewBuilder().CacheSQL(context.Background(), WithBuilderSQL(source), WithBuilderView(sourcePaginationView()), WithBuilderControls(controls), WithBuilderSelector(&xstate.Selector{Limit: 2, Offset: 2}))
	require.NoError(t, err)
	require.NotContains(t, strings.ToUpper(q.SQL), "LIMIT")
	require.NotContains(t, strings.ToUpper(q.SQL), "OFFSET")
	require.Contains(t, q.SQL, "ORDER BY name DESC")
	require.Equal(t, 2, q.Limit)
	require.Equal(t, 2, q.Offset)
	require.Equal(t, 2, *controls.Limit)
	require.Equal(t, 2, *controls.Offset)
}

func TestSourcePaginationProtectedTokens(t *testing.T) {
	for _, source := range []string{
		"SELECT '$PAGINATION' AS name FROM users -- $PAGINATION\n",
		"SELECT id AS \"$PAGINATION\" FROM users /* $PAGINATION */",
		"SELECT id AS `$PAGINATION` FROM users",
		"SELECT id AS [$PAGINATION] FROM users",
	} {
		require.Equal(t, source, paginationInspectionSource(source))
		got, consumed := prepareSourcePagination(source, nil)
		require.False(t, consumed)
		require.Equal(t, source, got)
	}
	source := "SELECT '$PAGINATION' AS name FROM users /* $PAGINATION */ $PAGINATION"
	cleaned := paginationInspectionSource(source)
	require.Contains(t, cleaned, "'$PAGINATION'")
	require.Contains(t, cleaned, "/* $PAGINATION */")
	require.NotContains(t, cleaned, "*/ $PAGINATION")
	limit := 2
	prepared, consumed := prepareSourcePagination(source, &spec.ViewControls{Limit: &limit})
	require.True(t, consumed)
	require.Contains(t, prepared, "'$PAGINATION'")
	require.Contains(t, prepared, "/* $PAGINATION */")
	require.Contains(t, prepared, "LIMIT 2")
}

func TestSourcePaginationProjectionErrorsRemainErrors(t *testing.T) {
	const source = "SELECT t.* FROM (SELECT id,name FROM users $PAGINATION) t"
	for _, mode := range []string{"Build", "ShapeBound"} {
		t.Run(mode, func(t *testing.T) {
			opts := []BuilderOption{WithBuilderSQL(source), WithBuilderView(sourcePaginationView()), WithBuilderProjection([]string{"missing"})}
			var err error
			if mode == "Build" {
				_, err = NewBuilder().Build(context.Background(), opts...)
			} else {
				_, err = NewBuilder().ShapeBound(&cache.ParmetrizedQuery{SQL: source}, opts...)
			}
			require.ErrorContains(t, err, "not found column")
		})
	}
}

func TestSourcePaginationPartitionArgumentsAndImmutableViewSQLite(t *testing.T) {
	ctx := context.Background()
	db := sqlite.New(t)
	require.NoError(t, db.ExecStatements(ctx, "CREATE TABLE events(id INTEGER,tenant_id INTEGER,shard INTEGER)", "INSERT INTO events VALUES(1,9,2),(2,9,1),(3,9,2),(4,9,2),(5,8,2)"))
	for _, mode := range []string{"Build", "ShapeBound"} {
		t.Run(mode, func(t *testing.T) {
			limit, offset := 2, 1
			source := &spec.ViewSource{Table: "events", SQL: "SELECT t.* FROM (SELECT id,tenant_id,shard FROM events WHERE tenant_id=:Tenant ORDER BY id $PAGINATION) t WHERE t.id<=:Max", Controls: &spec.ViewControls{Limit: &limit, Offset: &offset, OrderBy: "id DESC"}}
			originalSQL := source.SQL
			originalControls := source.Controls.Clone()
			view := &data.View{Spec: spec.View{Source: source}, Columns: []*data.Column{{Name: "ID", Column: "id"}, {Name: "Tenant", Column: "tenant_id"}, {Name: "Shard", Column: "shard"}}}
			partition := &PartitionInput{Expression: "t.shard = ?", Args: []any{2}}
			opts := []BuilderOption{WithBuilderView(view), WithBuilderProjection([]string{"id"}), WithBuilderPartition(partition), WithBuilderParameterResolver(parameterResolver(map[string]any{"tenant": 9, "max": 5}))}
			var q *cache.ParmetrizedQuery
			var err error
			if mode == "Build" {
				q, err = NewBuilder().Build(ctx, opts...)
			} else {
				bound := strings.ReplaceAll(strings.ReplaceAll(source.SQL, ":Tenant", "?"), ":Max", "?")
				q, err = NewBuilder().ShapeBound(&cache.ParmetrizedQuery{SQL: bound, Args: []any{9, 5}}, opts...)
			}
			require.NoError(t, err)
			require.Equal(t, []any{9, 5, 2}, q.Args, q.SQL)
			require.Equal(t, 1, strings.Count(strings.ToUpper(q.SQL), "LIMIT"))
			require.Equal(t, 1, strings.Count(strings.ToUpper(q.SQL), "OFFSET"))
			rows, err := db.DB.QueryContext(ctx, q.SQL, q.Args...)
			require.NoError(t, err, q.SQL)
			defer rows.Close()
			var ids []int
			for rows.Next() {
				var id int
				require.NoError(t, rows.Scan(&id))
				ids = append(ids, id)
			}
			require.NoError(t, rows.Err())
			require.Equal(t, []int{3}, ids)
			require.Same(t, source, view.Spec.Source)
			require.Equal(t, originalSQL, view.Spec.Source.SQL)
			require.Equal(t, originalControls, view.Spec.Source.Controls)
			require.Equal(t, []any{2}, partition.Args)
			require.Equal(t, "t.shard = ?", partition.Expression)
		})
	}
}

func TestSourcePaginationGroupedDefaultOrderSQLite(t *testing.T) {
	ctx := context.Background()
	db := sqlite.New(t)
	require.NoError(t, db.ExecStatements(ctx, "CREATE TABLE users(id INTEGER,tenant_id INTEGER,name TEXT)", "INSERT INTO users VALUES(1,1,'a'),(2,1,'b'),(3,2,'c'),(4,2,'d'),(5,3,'e')"))
	for _, mode := range []string{"Build", "ShapeBound"} {
		for _, tc := range []struct {
			name, order string
			want        []int
		}{
			{name: "pruned dimension", order: "name DESC", want: []int{1, 2}},
			{name: "retained dimension", order: "tenant_id DESC", want: []int{2, 1}},
		} {
			t.Run(mode+"/"+tc.name, func(t *testing.T) {
				limit, offset := 3, 1
				source := &spec.ViewSource{SQL: "SELECT tenant_id,name,COUNT(*) AS total FROM (SELECT * FROM users ORDER BY id $PAGINATION) w GROUP BY tenant_id,name", Controls: &spec.ViewControls{Limit: &limit, Offset: &offset, OrderBy: tc.order}}
				view := resolvedGroupableView()
				view.Spec.Source = source
				originalSQL := source.SQL
				originalControls := source.Controls.Clone()
				opts := []BuilderOption{WithBuilderView(view), WithBuilderProjection([]string{"tenant_id", "total"})}
				var q *cache.ParmetrizedQuery
				var err error
				if mode == "Build" {
					q, err = NewBuilder().Build(ctx, opts...)
				} else {
					q, err = NewBuilder().ShapeBound(&cache.ParmetrizedQuery{SQL: source.SQL}, opts...)
				}
				require.NoError(t, err)
				require.Equal(t, 1, strings.Count(strings.ToUpper(q.SQL), "LIMIT"))
				require.Equal(t, 1, strings.Count(strings.ToUpper(q.SQL), "OFFSET"))
				require.NotContains(t, q.SQL, "name DESC")
				if tc.name == "retained dimension" {
					require.Contains(t, q.SQL, "ORDER BY tenant_id DESC")
				}
				rows, err := db.DB.QueryContext(ctx, q.SQL, q.Args...)
				require.NoError(t, err, q.SQL)
				defer rows.Close()
				var tenants []int
				counts := map[int]int{}
				for rows.Next() {
					var tenant, count int
					require.NoError(t, rows.Scan(&tenant, &count))
					tenants = append(tenants, tenant)
					counts[tenant] = count
				}
				require.NoError(t, rows.Err())
				require.Equal(t, tc.want, tenants, q.SQL)
				require.Equal(t, map[int]int{1: 1, 2: 2}, counts)
				require.Equal(t, originalSQL, view.Spec.Source.SQL)
				require.Equal(t, originalControls, view.Spec.Source.Controls)
			})
		}
	}
}
