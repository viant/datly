package builder

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/viant/datly/data"
	"github.com/viant/datly/internal/testharness/sqlite"
	"github.com/viant/datly/spec"
	"github.com/viant/sqlx/io/read/cache"
	xstate "github.com/viant/xdatly/state"
)

func TestQualifiedSourceOrderingDistinctSQLite(t *testing.T) {
	ctx := context.Background()
	h := sqlite.New(t)
	require.NoError(t, h.ExecStatements(ctx, "CREATE TABLE items(id INTEGER,tenant_id INTEGER,label TEXT,owner_id INTEGER)", "CREATE TABLE owners(id INTEGER,name TEXT)", "INSERT INTO owners VALUES(1,'first'),(2,'second')", "INSERT INTO items VALUES(1,7,'alpha',1),(2,7,'beta',2),(3,7,NULL,1),(4,7,'alpha',1),(99,8,'outside',2)"))
	const inner = "SELECT DISTINCT p.label AS value FROM items p JOIN owners o ON p.owner_id=o.id WHERE p.tenant_id=:Tenant"
	const wrapped = "SELECT result.* FROM (" + inner + ") result"
	policy := &spec.Selector{AllowOrderBy: true, AllowLimit: true, AllowOffset: true, AllowPage: true, DefaultOrder: "p.id DESC", DefaultLimit: 100, Orderable: []spec.FieldPath{"p.id", "p.label", "value"}, OrderAliases: map[string]spec.FieldPath{"owner": "o.name"}}
	read := func(query string, args []any) []sql.NullString {
		rows, e := h.DB.QueryContext(ctx, query, args...)
		require.NoError(t, e)
		defer rows.Close()
		cols, e := rows.Columns()
		require.NoError(t, e)
		require.Equal(t, []string{"value"}, cols)
		var result []sql.NullString
		for rows.Next() {
			var value sql.NullString
			require.NoError(t, rows.Scan(&value))
			result = append(result, value)
		}
		require.NoError(t, rows.Err())
		return result
	}
	for _, tc := range []struct {
		name, order, expected string
		limit, offset, page   int
	}{
		{name: "default", expected: "p.id DESC"},
		{name: "terminal", order: "id", expected: "p.id"},
		{name: "qualified", order: "p.id:desc", expected: "p.id DESC"},
		{name: "joined alias", order: "owner", expected: "o.name"},
		{name: "mixed output", order: "id,value:desc", expected: "p.id, value DESC"},
		{name: "ordinal", order: "1 DESC", expected: "1 DESC"},
		{name: "after distinct window", order: "id", expected: "p.id", limit: 1, offset: 1},
		{name: "page", order: "id", expected: "p.id", limit: 1, page: 2},
	} {
		t.Run(tc.name, func(t *testing.T) {
			selector := &xstate.Selector{OrderBy: tc.order, Limit: tc.limit, Offset: tc.offset, Page: tc.page}
			opts := []BuilderOption{WithBuilderSQL(wrapped), WithBuilderSelectorPolicy(policy), WithBuilderSelector(selector), WithBuilderParameterResolver(parameterResolver(map[string]any{"tenant": 7}))}
			q, e := NewBuilder().Build(ctx, opts...)
			require.NoError(t, e)
			expected := inner + " ORDER BY " + tc.expected + " LIMIT 100"
			if tc.limit > 0 {
				expected = inner + " ORDER BY " + tc.expected + " LIMIT 1 OFFSET 1"
			}
			require.Equal(t, read(strings.ReplaceAll(expected, ":Tenant", "?"), []any{7}), read(q.SQL, interfaceSlice(q.Args)))
			require.Equal(t, []any{7}, interfaceSlice(q.Args))
			require.Equal(t, tc.order, selector.OrderBy)
			require.Equal(t, "p.id DESC", policy.DefaultOrder)
			bound, e := NewBuilder().ShapeBound(&cache.ParmetrizedQuery{SQL: strings.ReplaceAll(wrapped, ":Tenant", "?"), Args: []interface{}{7}}, WithBuilderSelectorPolicy(policy), WithBuilderSelector(selector))
			require.NoError(t, e)
			require.Equal(t, read(q.SQL, interfaceSlice(q.Args)), read(bound.SQL, interfaceSlice(bound.Args)))
			unwindow, e := NewBuilder().Build(ctx, append(opts, WithBuilderExcludePagination(true))...)
			require.NoError(t, e)
			require.NotContains(t, unwindow.SQL, "LIMIT")
			require.NotContains(t, unwindow.SQL, "OFFSET")
			require.Len(t, read(unwindow.SQL, interfaceSlice(unwindow.Args)), 3)
			matcher, e := NewBuilder().QueryMatcher(ctx, unwindow, WithBuilderSQL(unwindow.SQL), WithBuilderSelectorPolicy(policy), WithBuilderSelector(selector))
			require.NoError(t, e)
			require.Equal(t, unwindow.SQL, matcher.SQL)
			require.Equal(t, unwindow.Args, matcher.Args)
		})
	}
}

func TestQualifiedSourceOrderingGuards(t *testing.T) {
	const inner = "SELECT DISTINCT p.label AS value FROM items p WHERE p.tenant_id=:Tenant"
	const wrapped = "SELECT result.* FROM (" + inner + ") result"
	tests := []struct {
		name, source, order string
		allowed             []spec.FieldPath
		aliases             map[string]spec.FieldPath
	}{
		{name: "unlisted", source: wrapped, order: "id"},
		{name: "unqualified hidden", source: wrapped, order: "id", allowed: []spec.FieldPath{"id"}},
		{name: "unqualified alias", source: wrapped, order: "private", aliases: map[string]spec.FieldPath{"private": "id"}},
		{name: "wrong owner", source: wrapped, order: "q.id", allowed: []spec.FieldPath{"q.id"}},
		{name: "ambiguous terminal", source: wrapped, order: "id", allowed: []spec.FieldPath{"p.id", "q.id"}},
		{name: "injection", source: wrapped, order: "p.id; DROP TABLE items", allowed: []spec.FieldPath{"p.id"}},
		{name: "expression", source: wrapped, order: "LOWER(p.id)", allowed: []spec.FieldPath{"p.id"}},
		{name: "literal dot", source: wrapped, order: "\"p.id\"", allowed: []spec.FieldPath{"p.id"}},
		{name: "ordinal hidden", source: wrapped, order: "2", allowed: []spec.FieldPath{"p.id"}},
		{name: "filtered wrapper", source: wrapped + " WHERE value IS NOT NULL", order: "id", allowed: []spec.FieldPath{"p.id"}},
		{name: "windowed wrapper", source: wrapped + " LIMIT 2", order: "id", allowed: []spec.FieldPath{"p.id"}},
		{name: "computed wrapper", source: "SELECT COALESCE(result.value,'') AS value FROM (" + inner + ") result", order: "id", allowed: []spec.FieldPath{"p.id"}},
		{name: "nested unrelated", source: "SELECT result.* FROM (SELECT deeper.* FROM (" + inner + ") deeper) result", order: "id", allowed: []spec.FieldPath{"p.id"}},
		{name: "derived missing", source: "SELECT p.label AS value FROM (SELECT label FROM items) p", order: "id", allowed: []spec.FieldPath{"p.id"}},
		{name: "derived ambiguous", source: "SELECT p.label AS value FROM (SELECT label,id,id FROM items) p", order: "id", allowed: []spec.FieldPath{"p.id"}},
		{name: "duplicate owners", source: "SELECT p.label AS value FROM items p JOIN items p ON 1=1", order: "id", allowed: []spec.FieldPath{"p.id"}},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			_, e := NewBuilder().Build(context.Background(), WithBuilderSQL(tc.source), WithBuilderSelector(&xstate.Selector{OrderBy: tc.order}), WithBuilderSelectorPolicy(&spec.Selector{AllowOrderBy: true, Orderable: tc.allowed, OrderAliases: tc.aliases}), WithBuilderParameterResolver(parameterResolver(map[string]any{"tenant": 7})))
			require.Error(t, e)
		})
	}
	// Ordering permission cannot expand a requested output projection.
	_, e := NewBuilder().Build(context.Background(), WithBuilderSQL(wrapped), WithBuilderProjection([]string{"id"}), WithBuilderSelector(&xstate.Selector{OrderBy: "id"}), WithBuilderSelectorPolicy(&spec.Selector{AllowFields: true, AllowOrderBy: true, Orderable: []spec.FieldPath{"p.id"}}))
	require.Error(t, e)
}

func TestQualifiedSourceOrderInvocationIsolation(t *testing.T) {
	const source = "SELECT records.* FROM (SELECT DISTINCT p.label AS value FROM items p WHERE p.tenant_id=:Tenant) records"
	policy := &spec.Selector{AllowOrderBy: true, DefaultOrder: "p.id DESC", Orderable: []spec.FieldPath{"p.id"}}
	for i := 0; i < 24; i++ {
		t.Run(fmt.Sprint(i), func(t *testing.T) {
			t.Parallel()
			selector := &xstate.Selector{OrderBy: "id:asc"}
			q, e := NewBuilder().Build(context.Background(), WithBuilderSQL(source), WithBuilderSelectorPolicy(policy), WithBuilderSelector(selector), WithBuilderParameterResolver(parameterResolver(map[string]any{"tenant": i})))
			require.NoError(t, e)
			require.Contains(t, q.SQL, "ORDER BY p.id ASC")
			require.Equal(t, []any{i}, interfaceSlice(q.Args))
			require.Equal(t, "id:asc", selector.OrderBy)
			require.Equal(t, "p.id DESC", policy.DefaultOrder)
		})
	}
	// An ordering-only field is not a criteria field, even when explicitly named.
	_, e := NewBuilder().Build(context.Background(), WithBuilderSQL(source), WithBuilderSelectorPolicy(&spec.Selector{AllowCriteria: true, AllowOrderBy: true, Orderable: []spec.FieldPath{"p.id"}, Filterable: []spec.FieldPath{"id"}}), WithBuilderSelector(&xstate.Selector{OrderBy: "id", Criteria: "id = 1"}))
	require.Error(t, e)
	// One view's authorized qualifier cannot authorize another source's alias.
	_, e = NewBuilder().Build(context.Background(), WithBuilderSQL("SELECT q.label AS value FROM items q"), WithBuilderSelectorPolicy(policy), WithBuilderSelector(&xstate.Selector{OrderBy: "id"}))
	require.Error(t, e)
}

func TestQualifiedSourceOrderingPreservesConflictingScopes(t *testing.T) {
	const inner = "SELECT DISTINCT p.label AS value FROM items p"
	const wrapper = "SELECT r.* FROM (" + inner + ") r"
	policy := &spec.Selector{AllowOrderBy: true, AllowLimit: true, AllowOffset: true, AllowCriteria: true, DefaultOrder: "owner DESC", Orderable: []spec.FieldPath{"p.id"}, OrderAliases: map[string]spec.FieldPath{"owner": "p.id"}}
	q, e := NewBuilder().Build(context.Background(), WithBuilderSQL(wrapper), WithBuilderSelectorPolicy(policy))
	require.NoError(t, e)
	require.Equal(t, inner+" ORDER BY p.id DESC", q.SQL)
	// Source fallback is needed for default aliases even when no wrapper is removed.
	q, e = NewBuilder().Build(context.Background(), WithBuilderSQL(inner), WithBuilderSelectorPolicy(policy))
	require.NoError(t, e)
	require.Equal(t, inner+" ORDER BY p.id DESC", q.SQL)
	bound, e := NewBuilder().ShapeBound(&cache.ParmetrizedQuery{SQL: inner}, WithBuilderSelectorPolicy(policy))
	require.NoError(t, e)
	require.Equal(t, inner+" ORDER BY p.id DESC", bound.SQL)
	for _, source := range []string{inner, wrapper} {
		cached, err := NewBuilder().CacheSQL(context.Background(), WithBuilderSQL(source), WithBuilderSelectorPolicy(policy))
		require.NoError(t, err)
		require.Equal(t, inner+" ORDER BY p.id DESC", cached.SQL)
	}
	for _, late := range []BuilderOption{
		WithBuilderPartition(&PartitionInput{Expression: "r.value = ?", Args: []any{"a"}}),
		WithBuilderRelation(&data.Relation{}),
		func(o *builderOptions) {
			o.compositeColumns = []string{"r.value"}
			o.compositeRows = [][]interface{}{{"a"}}
		},
		func(o *builderOptions) { o.forUpdate = true },
	} {
		_, err := NewBuilder().Build(context.Background(), WithBuilderSQL(wrapper), WithBuilderSelectorPolicy(policy), late)
		require.ErrorContains(t, err, "deferred partition, relation or lock")
	}
	for _, tc := range []struct{ name, source, criteria string }{
		{"selector criteria", wrapper, "r.value = 'a'"},
		{"authored pagination", "SELECT r.* FROM (" + inner + " $PAGINATION) r", ""},
		{"authored order", "SELECT r.* FROM (" + inner + " ORDER BY p.id DESC) r", ""},
		{"authored limit", "SELECT r.* FROM (" + inner + " LIMIT 2) r", ""},
	} {
		t.Run(tc.name, func(t *testing.T) {
			_, e := NewBuilder().Build(context.Background(), WithBuilderSQL(tc.source), WithBuilderSelectorPolicy(policy), WithBuilderSelector(&xstate.Selector{OrderBy: "id", Limit: 1, Criteria: tc.criteria}))
			require.Error(t, e)
		})
	}
	// CacheSQL builds its non-window query in the same source scope and retains
	// only the requested matcher window, without mutating the registered policy.
	q, e = NewBuilder().CacheSQL(context.Background(), WithBuilderSQL(wrapper), WithBuilderSelectorPolicy(policy), WithBuilderSelector(&xstate.Selector{OrderBy: "id", Limit: 1, Offset: 1}))
	require.NoError(t, e)
	require.Equal(t, inner+" ORDER BY p.id", q.SQL)
	require.Equal(t, 1, q.Limit)
	require.Equal(t, 1, q.Offset)
	require.Equal(t, "owner DESC", policy.DefaultOrder)
}
