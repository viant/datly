package builder

import (
	"context"
	"github.com/stretchr/testify/require"
	"github.com/viant/datly/data"
	"github.com/viant/datly/internal/testharness/sqlite"
	"github.com/viant/datly/spec"
	xstate "github.com/viant/xdatly/state"
	"strings"
	"testing"
)

func TestSelectorNamePairwiseAuthority(t *testing.T) {
	names := []string{`"b.id"`, "b_id", "bid"}
	for _, source := range names {
		for _, requested := range names {
			t.Run(source+"/"+requested, func(t *testing.T) {
				sql := "SELECT id AS " + source + " FROM users b"
				for _, mode := range []string{"fields", "order", "policy", "ordinal"} {
					t.Run(mode, func(t *testing.T) {
						policy := &spec.Selector{AllowFields: true, AllowOrderBy: true}
						selection := &xstate.Selector{}
						opts := []BuilderOption{WithBuilderSQL(sql), WithBuilderSelectorPolicy(policy), WithBuilderSelector(selection)}
						switch mode {
						case "fields":
							opts = append(opts, WithBuilderProjection([]string{requested}))
						case "order":
							selection.OrderBy = requested
						case "policy":
							selection.OrderBy = source
							policy.Orderable = []spec.FieldPath{spec.FieldPath(requested)}
						case "ordinal":
							selection.OrderBy = "1"
							policy.Orderable = []spec.FieldPath{spec.FieldPath(requested)}
						}
						_, err := NewBuilder().Build(context.Background(), opts...)
						if source == requested {
							require.NoError(t, err)
						} else {
							require.Error(t, err)
						}
					})
				}
				// Each different spelling is usable with explicit mapping authority.
				mapping := &data.View{Spec: spec.View{Columns: []*spec.Column{{Name: requested, Source: source}}}}
				_, err := NewBuilder().Build(context.Background(), WithBuilderView(mapping), WithBuilderSQL(sql), WithBuilderProjection([]string{requested}), WithBuilderSelector(&xstate.Selector{OrderBy: requested}), WithBuilderSelectorPolicy(&spec.Selector{AllowFields: true, AllowOrderBy: true}))
				require.NoError(t, err)
				_, err = NewBuilder().Build(context.Background(), WithBuilderSQL(sql), WithBuilderSelector(&xstate.Selector{OrderBy: requested}), WithBuilderSelectorPolicy(&spec.Selector{AllowOrderBy: true, OrderAliases: map[string]spec.FieldPath{requested: spec.FieldPath(source)}}))
				require.NoError(t, err)
			})
		}
	}
}

func TestSelectorQuotedAndWildcardAuthoritySQLite(t *testing.T) {
	ctx := context.Background()
	db := sqlite.New(t)
	require.NoError(t, db.ExecStatements(ctx, `CREATE TABLE users(id INTEGER,b_id INTEGER,bid INTEGER,"odd.name" INTEGER)`, `INSERT INTO users VALUES(1,2,3,4)`))
	for _, tc := range []struct {
		name, source, field, order string
		columns                    []*data.Column
		wantCols                   []string
		want                       []any
		reject                     bool
	}{
		{name: "quoted literal dot", source: `SELECT id AS "b.id" FROM users`, field: `"b.id"`, order: `"b.id" DESC`, wantCols: []string{"b.id"}, want: []any{int64(1)}},
		{name: "quoted literal is not qualification", source: `SELECT id AS "b.id" FROM users`, field: "b.id", reject: true},
		{name: "qualified is not quoted literal", source: `SELECT b.id FROM users b`, field: `"b.id"`, reject: true},
		{name: "quoted qualifier", source: `SELECT "b"."id" FROM users b`, field: `id`, order: `id DESC`, wantCols: []string{"id"}, want: []any{int64(1)}},
		{name: "quoted terminal dot", source: `SELECT b."odd.name" FROM users b`, field: `"odd.name"`, order: `"odd.name"`, wantCols: []string{"odd.name"}, want: []any{int64(4)}},
		{name: "terminal dot not extra scope", source: `SELECT b."odd.name" FROM users b`, field: "b.odd.name", reject: true},
		{name: "terminal dot not short terminal", source: `SELECT b."odd.name" FROM users b`, field: "name", reject: true},
		{name: "qualified wildcard no invented alias", source: "SELECT b.* FROM users b", field: "id", order: "id", columns: []*data.Column{{Name: "BID", Column: "b.id"}}, wantCols: []string{"id"}, want: []any{int64(1)}},
		{name: "qualified wildcard refuses inferred alias", source: "SELECT b.* FROM users b", field: "bid", columns: []*data.Column{{Name: "BID", Column: "b.id"}}, reject: true},
		{name: "mixed colliding explicit alias", source: "SELECT b.*,99 AS bid FROM users b", field: "id", order: "id", columns: []*data.Column{{Name: "BID", Column: "b.id"}}, wantCols: []string{"id"}, want: []any{int64(1)}},
		{name: "mixed explicit alias owned", source: "SELECT b.*,99 AS bid FROM users b", field: "bid", order: "bid", columns: []*data.Column{{Name: "BID", Column: "b.id"}}, wantCols: []string{"bid"}, want: []any{int64(99)}},
		{name: "CTE explicit underscore alias", source: "WITH b AS (SELECT id AS b_id FROM users) SELECT b.* FROM b", field: "b_id", order: "b_id", wantCols: []string{"b_id"}, want: []any{int64(1)}},
		{name: "CTE cannot flatten", source: "WITH b AS (SELECT id AS b_id FROM users) SELECT b.* FROM b", field: "bid", reject: true},
		{name: "wildcard quoted metadata dot", source: "SELECT b.* FROM users b", field: `"odd.name"`, order: `"odd.name"`, columns: []*data.Column{{Name: "OddName", Column: `b."odd.name"`}}, wantCols: []string{"odd.name"}, want: []any{int64(4)}},
		{name: "EXCEPT does not flatten", source: "SELECT b.* EXCEPT(b_id) FROM users b", field: "bid", order: "1", columns: []*data.Column{{Name: "BID", Column: "b.id"}, {Name: "Underscore", Column: "b.b_id"}, {Name: "Collapsed", Column: "b.bid"}}, wantCols: []string{"bid"}, want: []any{int64(3)}},
		{name: "EXCEPT excludes exact output", source: "SELECT b.* EXCEPT(b_id) FROM users b", field: "b_id", columns: []*data.Column{{Name: "ID", Column: "b.id"}, {Name: "Underscore", Column: "b.b_id"}, {Name: "Collapsed", Column: "b.bid"}}, reject: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			q, err := NewBuilder().Build(ctx, WithBuilderView(&data.View{Columns: tc.columns}), WithBuilderSQL(tc.source), WithBuilderProjection([]string{tc.field}), WithBuilderSelector(&xstate.Selector{OrderBy: tc.order}), WithBuilderSelectorPolicy(&spec.Selector{AllowFields: true, AllowOrderBy: true}))
			if tc.reject {
				require.Error(t, err)
				return
			}
			require.NoError(t, err)
			rows, err := db.DB.QueryContext(ctx, q.SQL, q.Args...)
			require.NoError(t, err, q.SQL)
			defer rows.Close()
			cols, err := rows.Columns()
			require.NoError(t, err)
			require.Equal(t, tc.wantCols, cols, q.SQL)
			require.True(t, rows.Next())
			values := make([]any, len(cols))
			dest := make([]any, len(cols))
			for i := range dest {
				dest[i] = &values[i]
			}
			require.NoError(t, rows.Scan(dest...))
			require.Equal(t, tc.want, values)
			if strings.Contains(tc.name, "no invented") {
				require.NotContains(t, q.SQL, "AS BID")
			}
		})
	}
}

func TestTableAliasesNeedCanonicalAuthority(t *testing.T) {
	for _, authored := range []bool{false, true} {
		view := &data.View{Spec: spec.View{Source: &spec.ViewSource{Table: "users"}}, Columns: []*data.Column{{Name: "bid", Column: "id"}}}
		if authored {
			view.Spec.Columns = []*spec.Column{{Name: "bid", Source: "id"}}
		}
		b := NewBuilder()
		q, err := b.Build(context.Background(), WithBuilderView(view))
		require.NoError(t, err)
		if authored {
			require.Equal(t, "SELECT id AS bid FROM users", q.SQL)
		} else {
			require.Equal(t, "SELECT id FROM users", q.SQL)
		}
		_, err = b.Build(context.Background(), WithBuilderView(view), WithBuilderProjection([]string{"bid"}))
		if authored {
			require.NoError(t, err)
		} else {
			require.Error(t, err)
		}
	}
}

func TestDeclaredOutputsAndDuplicateNames(t *testing.T) {
	ctx := context.Background()
	for _, source := range []string{
		"SELECT a.id,b.id FROM users a JOIN users b ON a.id=b.id",
		"SELECT id AS bid,b_id AS bid FROM users",
		"SELECT id AS ID,b_id AS id FROM users",
		"SELECT a.*,b.* FROM users a JOIN users b ON a.id=b.id",
		"WITH a AS (SELECT id FROM users),b AS (SELECT id FROM users) SELECT a.*,b.* FROM a JOIN b ON a.id=b.id",
	} {
		for _, fields := range [][]string{nil, {"id"}, {"b.id"}} {
			t.Run(source+strings.Join(fields, ","), func(t *testing.T) {
				_, err := NewBuilder().Build(ctx, WithBuilderSQL(source), WithBuilderView(&data.View{Columns: []*data.Column{{Column: "a.id", Name: "AID"}, {Column: "b.id", Name: "BID"}}}), WithBuilderSQL(source), WithBuilderProjection(fields))
				require.ErrorContains(t, err, "duplicate output column")
				require.ErrorContains(t, err, "assign distinct SQL aliases")
			})
		}
	}
	for _, source := range []string{"SELECT b.id FROM users b", "SELECT b_id AS bid FROM users"} {
		output, hidden := "id", "b.id"
		if strings.Contains(source, " AS ") {
			output = "bid"
			hidden = "b_id"
		}
		for _, requested := range []string{output, hidden} {
			_, err := NewBuilder().Build(ctx, WithBuilderSQL(source), WithBuilderProjection([]string{requested}), WithBuilderSelector(&xstate.Selector{OrderBy: requested}))
			if requested == output {
				require.NoError(t, err)
			} else {
				require.Error(t, err)
			}
		}
		_, err := NewBuilder().Build(ctx, WithBuilderSQL(source), WithBuilderSelector(&xstate.Selector{OrderBy: "1"}), WithBuilderSelectorPolicy(&spec.Selector{AllowOrderBy: true, Orderable: []spec.FieldPath{spec.FieldPath(hidden)}}))
		require.ErrorContains(t, err, "not allowed")
	}
	// One projection can expose all three distinct names with explicit aliases.
	_, err := NewBuilder().Build(ctx, WithBuilderSQL(`SELECT id AS "b.id",b_id AS b_id,bid AS bid FROM users`), WithBuilderProjection([]string{`"b.id"`, "b_id", "bid"}), WithBuilderSelector(&xstate.Selector{OrderBy: "3"}))
	require.NoError(t, err)
}

func TestOrderMappingTargetIsAnActualOutput(t *testing.T) {
	view := &data.View{Spec: spec.View{Columns: []*spec.Column{{Name: "intermediate", Source: "id"}}}}
	policy := &spec.Selector{AllowFields: true, AllowOrderBy: true, OrderAliases: map[string]spec.FieldPath{"display": "intermediate"}}
	for _, order := range []string{"display", "1"} {
		_, err := NewBuilder().Build(context.Background(), WithBuilderView(view), WithBuilderSQL("SELECT id FROM users"), WithBuilderSelectorPolicy(policy), WithBuilderSelector(&xstate.Selector{OrderBy: order}))
		require.Error(t, err)
	}
	policy.OrderAliases["display"] = "id"
	_, err := NewBuilder().Build(context.Background(), WithBuilderView(view), WithBuilderSQL("SELECT id FROM users"), WithBuilderSelectorPolicy(policy), WithBuilderSelector(&xstate.Selector{OrderBy: "display"}))
	require.NoError(t, err)
}

func TestCanonicalTableMappingKeepsNativeMatcherName(t *testing.T) {
	ctx := context.Background()
	view := &data.View{Spec: spec.View{Namespace: "b", Source: &spec.ViewSource{Table: "users"}, Columns: []*spec.Column{{Name: "bid", Source: "b.id"}}}, Columns: []*data.Column{{Name: "bid", Column: "b.id"}}}
	opts := []BuilderOption{WithBuilderView(view), WithBuilderMatcher("b.id", []any{1})}
	b := NewBuilder()
	q, err := b.Build(ctx, opts...)
	require.NoError(t, err)
	require.Equal(t, "bid", q.By)
	require.Contains(t, q.SQL, "b.id AS bid")
	cached, err := b.CacheSQL(ctx, opts...)
	require.NoError(t, err)
	require.Equal(t, "bid", cached.By)
	matched, err := b.QueryMatcher(ctx, q, opts...)
	require.NoError(t, err)
	require.Equal(t, "bid", matched.By)
}
