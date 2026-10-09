package builder

import (
	"context"
	"reflect"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/viant/datly/data"
	"github.com/viant/datly/internal/testharness/sqlite"
	"github.com/viant/datly/spec"
	sqltemplate "github.com/viant/datly/sql/template"
	"github.com/viant/sqlx/io/read/cache"
	"github.com/viant/xdatly/response"
	xstate "github.com/viant/xdatly/state"
)

func sourcePaginationView() *data.View {
	return &data.View{Columns: []*data.Column{{Name: "ID", Column: "id"}, {Name: "Name", Column: "name"}}}
}

func TestTemplateWildcardNonWindowMatcherSQLite(t *testing.T) {
	type input struct{ Tenant, Minimum int }
	ctx := context.Background()
	h := sqlite.New(t)
	require.NoError(t, h.ExecStatements(ctx, "CREATE TABLE matcher_records(id INTEGER,tenant_id INTEGER,name TEXT)", "INSERT INTO matcher_records VALUES(1,7,'alpha'),(2,7,'gamma'),(3,7,'beta'),(4,7,'omit'),(99,8,'outside')"))
	const source = `SELECT * FROM (#set($constant = 1) SELECT id,name FROM matcher_records WHERE tenant_id=$criteria.AppendBinding($Unsafe.Tenant) AND id>$criteria.AppendBinding($Unsafe.Minimum)) rows $WHERE_SELECTOR_CRITERIA`
	evaluator, err := (sqltemplate.Compiler{Source: source, InputType: reflect.TypeFor[input]()}).Compile()
	require.NoError(t, err)
	for _, tc := range []struct {
		name, order string
		orderable   []spec.FieldPath
		deny        string
	}{
		{name: "name order", order: "name DESC"},
		{name: "ordinal order", order: "2 DESC"},
		{name: "unknown order", order: "missing", deny: "not in source projection"},
		{name: "disallowed name", order: "id", orderable: []spec.FieldPath{"name"}, deny: "not allowed"},
		{name: "disallowed ordinal", order: "1", orderable: []spec.FieldPath{"name"}, deny: "not allowed"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			view := &data.View{Spec: spec.View{Name: "Rows", Namespace: "rows", Source: &spec.ViewSource{SQL: source}, Selector: &spec.Selector{AllowOrderBy: true, AllowLimit: true, AllowOffset: true, AllowCriteria: true, Filterable: []spec.FieldPath{"name"}, Orderable: tc.orderable}}, Columns: []*data.Column{{Name: "ID", Column: "id", Output: "id"}, {Name: "Name", Column: "name", Output: "name"}}}
			// Build a full, bound root first. Ordering is validated separately by
			// the matcher against this evaluated SQL, not the authored template.
			selector := &xstate.Selector{Limit: 1, Offset: 1, Criteria: "name <> ?", Placeholders: []any{"omit"}}
			b := NewBuilder()
			query, err := b.Build(ctx, WithBuilderView(view), WithBuilderSelector(NonWindowSelector(selector)), WithBuilderTemplate(evaluator), WithBuilderInput(reflect.ValueOf(input{Tenant: 7, Minimum: 1})), WithBuilderExcludePagination(true))
			require.NoError(t, err)
			require.Equal(t, []any{7, 1, "omit"}, query.Args)
			require.NotContains(t, query.SQL, "#set")
			require.NotContains(t, query.SQL, "$criteria")
			require.NotContains(t, strings.ToUpper(query.SQL), "LIMIT")
			require.NotContains(t, strings.ToUpper(query.SQL), "OFFSET")
			selector.OrderBy = tc.order
			matched, err := b.QueryMatcher(ctx, query, WithBuilderView(view), WithBuilderSQL(query.SQL), WithBuilderSelector(selector))
			if tc.deny != "" {
				require.ErrorContains(t, err, tc.deny)
				var failure *response.Error
				require.ErrorAs(t, err, &failure)
				require.Equal(t, 400, failure.Code)
				return
			}
			require.NoError(t, err)
			require.Equal(t, query.SQL, matched.SQL)
			require.Equal(t, query.Args, matched.Args)
			require.Equal(t, 1, matched.Limit)
			require.Equal(t, 1, matched.Offset)
			require.Equal(t, source, view.Spec.Source.SQL)
			var count, sum, max int
			require.NoError(t, h.DB.QueryRowContext(ctx, "SELECT COUNT(*),SUM(id),MAX(id) FROM ("+matched.SQL+") parent", matched.Args...).Scan(&count, &sum, &max))
			require.Equal(t, []int{2, 5, 3}, []int{count, sum, max})
			// Repeated derived-source occurrences each retain their own binding
			// segment; surrounding bindings cannot move into the root segment.
			component := &spec.Component{Name: "Records", RootView: &spec.View{Name: "Rows"}}
			relation := mustSummaryRelation(t, view, &spec.ViewSource{SQL: "SELECT :before,(SELECT SUM(id) FROM ($View.Rows.NonWindowSQL) a),(SELECT MAX(id) FROM ($View.NonWindowSQL) b),:after"}, reflect.TypeFor[struct{ Value int }]())
			bound, args, err := (PreparedRelationBinder{Component: component, Relation: relation, RootNonWindowSQL: matched.SQL, RootArgs: matched.Args, ParameterResolver: parameterResolver(map[string]any{"before": 51, "after": 52})}).Bind()
			require.NoError(t, err)
			require.Equal(t, []any{51, 7, 1, "omit", 7, 1, "omit", 52}, args)
			var before, after int
			require.NoError(t, h.DB.QueryRowContext(ctx, bound, args...).Scan(&before, &sum, &max, &after))
			require.Equal(t, []int{51, 5, 3, 52}, []int{before, sum, max, after})
		})
	}
}

func TestTemplateWildcardProjectedOrdinalWindowSQLite(t *testing.T) {
	type input struct{ Tenant int }
	ctx := context.Background()
	h := sqlite.New(t)
	require.NoError(t, h.ExecStatements(ctx, "CREATE TABLE ordinal_records(id INTEGER,tenant_id INTEGER,name TEXT)", "INSERT INTO ordinal_records VALUES(1,7,'alpha'),(2,7,'gamma'),(3,7,'beta')"))
	const source = `SELECT * FROM (#set($constant = 1) SELECT id,name FROM ordinal_records WHERE tenant_id=$criteria.AppendBinding($Unsafe.Tenant)) rows`
	evaluator, err := (sqltemplate.Compiler{Source: source, InputType: reflect.TypeFor[input]()}).Compile()
	require.NoError(t, err)
	view := &data.View{Spec: spec.View{Name: "Rows", Namespace: "rows", Source: &spec.ViewSource{SQL: source}, Selector: &spec.Selector{AllowFields: true, AllowOrderBy: true, AllowLimit: true, AllowOffset: true, Orderable: []spec.FieldPath{"name"}}}, Columns: []*data.Column{{Name: "ID", Column: "id", Output: "id"}, {Name: "Name", Column: "name", Output: "name"}}}
	selector := &xstate.Selector{Fields: []string{"name"}, OrderBy: "1 DESC", Limit: 1, Offset: 1}
	b := NewBuilder()
	opts := []BuilderOption{WithBuilderView(view), WithBuilderTemplate(evaluator), WithBuilderInput(reflect.ValueOf(input{Tenant: 7}))}
	main, err := b.Build(ctx, append(append([]BuilderOption{}, opts...), WithBuilderProjection(selector.Fields), WithBuilderSelector(selector))...)
	require.NoError(t, err, "projected main Build stage")
	var name string
	require.NoError(t, h.DB.QueryRowContext(ctx, main.SQL, main.Args...).Scan(&name))
	require.Equal(t, "beta", name)
	t.Logf("projected main Build passed: SQL=%s args=%v", main.SQL, main.Args)
	nonWindowSelector := NonWindowSelector(selector).Clone()
	nonWindowSelector.Columns = append([]string(nil), selector.Fields...)
	nonWindowSelector.Fields = nil
	nonWindow, err := b.Build(ctx, append(append([]BuilderOption{}, opts...), WithBuilderSelector(nonWindowSelector), WithBuilderExcludePagination(true))...)
	require.NoError(t, err, "full-column nonWindow Build stage must retain projected ordinal meaning")
	matcherSelector := selector.Clone()
	matcherSelector.OrderBy = ""
	_, err = b.QueryMatcher(ctx, nonWindow, WithBuilderView(view), WithBuilderSQL(nonWindow.SQL), WithBuilderSelector(matcherSelector))
	require.NoError(t, err, "evaluated nonWindow QueryMatcher stage")
}

func TestNonWindowOrdinalReferencePreservesProjectionOrderSQLite(t *testing.T) {
	ctx := context.Background()
	h := sqlite.New(t)
	require.NoError(t, h.ExecStatements(ctx, "CREATE TABLE ordinal_reference(id INTEGER,name TEXT,rank INTEGER)", "INSERT INTO ordinal_reference VALUES(1,'b',1),(2,'a',2),(3,'a',1)"))
	for _, tc := range []struct {
		name, source, order, wantOrder string
		columns                        []string
		allowed                        []spec.FieldPath
		wantIDs                        []int
		reject                         bool
	}{
		{name: "wildcard reordered selection", source: "SELECT r.* FROM (SELECT id,name,rank FROM ordinal_reference WHERE id>?) r", columns: []string{"rank", "name"}, order: "1 DESC,2 ASC", wantOrder: "ORDER BY 3 DESC, 2 ASC", wantIDs: []int{2, 3, 1}},
		{name: "explicit source preserves source order", source: "SELECT id,name,rank FROM ordinal_reference WHERE id>?", columns: []string{"rank", "name"}, order: "1 DESC,2 ASC", wantOrder: "ORDER BY 2 DESC, 3 ASC", wantIDs: []int{1, 3, 2}},
		{name: "mixed ordinal and name", columns: []string{"rank", "name"}, order: "1 DESC,name ASC", wantOrder: "ORDER BY 3 DESC, r.name ASC", wantIDs: []int{2, 3, 1}},
		{name: "named order ignores ordinal reference", columns: []string{"missing"}, order: "id DESC", wantOrder: "ORDER BY r.id DESC", wantIDs: []int{3, 2, 1}},
		{name: "zero", columns: []string{"name"}, order: "0", reject: true},
		{name: "negative", columns: []string{"name"}, order: "-1", reject: true},
		{name: "overflow", columns: []string{"name"}, order: "9999999999999999999999999999999999999", reject: true},
		{name: "out of selected range", columns: []string{"name"}, order: "2", reject: true},
		{name: "disallowed selected column", columns: []string{"rank", "name"}, order: "1", allowed: []spec.FieldPath{"name"}, reject: true},
		{name: "unknown selected column", columns: []string{"missing"}, order: "1", reject: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if tc.source == "" {
				tc.source = "SELECT r.* FROM (SELECT id,name,rank FROM ordinal_reference WHERE id>?) r"
			}
			view := &data.View{Spec: spec.View{Name: "records", Source: &spec.ViewSource{SQL: tc.source}, Selector: &spec.Selector{AllowOrderBy: true, Orderable: tc.allowed}}, Columns: []*data.Column{{Name: "ID", Column: "id"}, {Name: "Name", Column: "name"}, {Name: "Rank", Column: "rank"}}}
			selector := &xstate.Selector{Columns: tc.columns, OrderBy: tc.order}
			original := selector.Clone()
			args := []any{0}
			q, err := NewBuilder().Build(ctx, WithBuilderView(view), WithBuilderSelector(selector), WithBuilderPositionalArgs(args), WithBuilderExcludePagination(true))
			require.Equal(t, original, selector)
			require.Equal(t, tc.source, view.Spec.Source.SQL)
			require.Equal(t, []any{0}, args)
			if tc.reject {
				var failure *response.Error
				require.ErrorAs(t, err, &failure)
				require.Equal(t, 400, failure.Code)
				return
			}
			require.NoError(t, err)
			require.Contains(t, q.SQL, tc.wantOrder)
			require.Equal(t, []any{0}, q.Args)
			rows, err := h.DB.QueryContext(ctx, q.SQL, q.Args...)
			require.NoError(t, err)
			defer rows.Close()
			columns, err := rows.Columns()
			require.NoError(t, err)
			require.Equal(t, []string{"id", "name", "rank"}, columns)
			var ids []int
			for rows.Next() {
				var id, rank int
				var name string
				require.NoError(t, rows.Scan(&id, &name, &rank))
				ids = append(ids, id)
			}
			require.NoError(t, rows.Err())
			require.Equal(t, tc.wantIDs, ids)
		})
	}
}

func TestOrdinalReferenceAppliesOnlyToFullNonWindowQuery(t *testing.T) {
	for _, tc := range []struct {
		name       string
		exclude    bool
		projection []string
		want       string
	}{
		{name: "ordinary query", want: "ORDER BY 1 DESC"},
		{name: "projected nonwindow query", exclude: true, projection: []string{"id"}, want: "ORDER BY 1 DESC"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			q, err := NewBuilder().Build(context.Background(), WithBuilderSQL("SELECT id,name FROM records"), WithBuilderProjection(tc.projection), WithBuilderSelectorPolicy(&spec.Selector{AllowFields: true, AllowOrderBy: true}), WithBuilderSelector(&xstate.Selector{Columns: []string{"name"}, OrderBy: "1 DESC"}), WithBuilderExcludePagination(tc.exclude))
			require.NoError(t, err)
			require.Contains(t, q.SQL, tc.want)
		})
	}
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
