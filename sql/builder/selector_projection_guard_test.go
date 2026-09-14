package builder

import (
	"context"
	"github.com/stretchr/testify/require"
	"github.com/viant/datly/data"
	"github.com/viant/datly/internal/testharness/sqlite"
	"github.com/viant/datly/spec"
	xstate "github.com/viant/xdatly/state"
	"testing"
)

func TestOrderRequiresProjectedColumn(t *testing.T) {
	for _, tc := range []struct {
		name, sql, order string
		allowed          []spec.FieldPath
		columns          []*data.Column
		reject           bool
	}{
		{name: "known column", sql: "SELECT id,name FROM users", order: "name"},
		{name: "unknown", sql: "SELECT id,name FROM users", order: "secret", reject: true},
		{name: "allowlist cannot add column", sql: "SELECT id,name FROM users", order: "secret", allowed: []spec.FieldPath{"secret"}, reject: true},
		{name: "unresolved wildcard", sql: "SELECT * FROM users", order: "secret", reject: true},
		{name: "prepared wildcard", sql: "SELECT * FROM users", order: "name", columns: []*data.Column{{Name: "id"}, {Name: "name"}}},
		{name: "prepared wildcard unknown", sql: "SELECT * FROM users", order: "secret", columns: []*data.Column{{Name: "id"}, {Name: "name"}}, reject: true},
		{name: "ordinal in range", sql: "SELECT id,name FROM users", order: "2"},
		{name: "ordinal out of range", sql: "SELECT id,name FROM users", order: "99", reject: true},
		{name: "ordinal zero", sql: "SELECT id,name FROM users", order: "0", reject: true},
		{name: "ordinal cannot bypass allowlist", sql: "SELECT id,name FROM users", order: "2", allowed: []spec.FieldPath{"id"}, reject: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			opts := []BuilderOption{WithBuilderSQL(tc.sql), WithBuilderSelectorPolicy(&spec.Selector{AllowOrderBy: true, Orderable: tc.allowed}), WithBuilderSelector(&xstate.Selector{OrderBy: tc.order})}
			if tc.columns != nil {
				opts = append([]BuilderOption{WithBuilderView(&data.View{Columns: tc.columns})}, opts...)
			}
			_, err := NewBuilder().Build(context.Background(), opts...)
			if tc.reject {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
			}
		})
	}
}

func TestOrderPositionRespectsNarrowedProjection(t *testing.T) {
	for _, order := range []string{"1", "2"} {
		t.Run(order, func(t *testing.T) {
			_, err := NewBuilder().Build(context.Background(), WithBuilderSQL("SELECT id,name FROM users"), WithBuilderSelectorPolicy(&spec.Selector{AllowFields: true, AllowOrderBy: true}), WithBuilderProjection([]string{"name"}), WithBuilderSelector(&xstate.Selector{OrderBy: order}))
			if order == "1" {
				require.NoError(t, err)
			} else {
				require.Error(t, err)
			}
		})
	}
}

func TestOrderingUsesProjectionVocabularyAndExclusions(t *testing.T) {
	for _, tc := range []struct {
		name, sql, order string
		fields           []string
		columns          []*data.Column
		reject           bool
	}{
		{name: "terminal field", sql: "SELECT users.name FROM users", order: "1", fields: []string{"name"}},
		{name: "canonical alias", sql: "SELECT users.name AS display_name FROM users", order: "1", fields: []string{"display_name"}},
		{name: "named alias", sql: "SELECT users.name AS display_name FROM users", order: "display_name"},
		{name: "excluded order", sql: "SELECT * EXCEPT(secret) FROM users", order: "secret", columns: []*data.Column{{Name: "id"}, {Name: "secret"}}, reject: true},
		{name: "excluded selection", sql: "SELECT * EXCEPT(secret) FROM users", order: "1", fields: []string{"secret"}, columns: []*data.Column{{Name: "id"}, {Name: "secret"}}, reject: true},
		{name: "retained field", sql: "SELECT * EXCEPT(secret) FROM users", order: "id", columns: []*data.Column{{Name: "id"}, {Name: "secret"}}},
		{name: "ambiguous terminal", sql: "SELECT a.id,b.id FROM a JOIN b ON a.id=b.id", order: "id", reject: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			opts := []BuilderOption{WithBuilderSQL(tc.sql), WithBuilderSelectorPolicy(&spec.Selector{AllowOrderBy: true, AllowFields: true}), WithBuilderSelector(&xstate.Selector{OrderBy: tc.order}), WithBuilderProjection(tc.fields)}
			if tc.columns != nil {
				opts = append([]BuilderOption{WithBuilderView(&data.View{Columns: tc.columns})}, opts...)
			}
			_, err := NewBuilder().Build(context.Background(), opts...)
			if tc.reject {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
			}
		})
	}
}

func TestSourceGuardRenderedOrderSQLite(t *testing.T) {
	ctx := context.Background()
	h := sqlite.New(t)
	require.NoError(t, h.ExecStatements(ctx, "CREATE TABLE guard_order(id INTEGER,name TEXT,secret TEXT)", "INSERT INTO guard_order VALUES(1,'z','x'),(2,'a','y')"))
	for _, tc := range []struct {
		name, source, order, wantSQL string
		fields                       []string
		columns                      []*data.Column
		allowed                      []spec.FieldPath
		want                         []any
	}{
		{name: "explicit source order", source: "SELECT id,name FROM guard_order", fields: []string{"name", "id"}, order: "1 DESC", wantSQL: "SELECT id,name FROM guard_order ORDER BY 1 DESC", want: []any{int64(2), "a"}},
		{name: "mixed expanded order", source: "SELECT u.*,7 AS extra FROM (SELECT id,name FROM guard_order) u", fields: []string{"extra", "name"}, order: "1 DESC", wantSQL: "SELECT u.name, 7 AS extra FROM (SELECT id,name FROM guard_order) u ORDER BY 1 DESC", want: []any{"z", int64(7)}},
		{name: "qualified joined selection", source: "SELECT a.*,b.* FROM (SELECT id,name FROM guard_order) a JOIN (SELECT id AS b_id FROM guard_order) b ON a.id=b.b_id", fields: []string{"name", "b_id"}, order: "1 DESC", want: []any{"z", int64(1)}},
		{name: "selected alias preserves underscores", source: "SELECT id,u.name AS display_name FROM guard_order u", fields: []string{"Display_Name"}, order: "DISPLAY_NAME DESC", wantSQL: "SELECT u.name AS display_name FROM guard_order u ORDER BY display_name DESC", want: []any{"z"}},
		{name: "qualified wildcard joined order", source: "SELECT a.* FROM (SELECT id,name FROM guard_order) a JOIN guard_order b ON a.id=b.id", order: "id DESC", want: []any{int64(2), "a"}},
		{name: "derived no alias", source: "SELECT * FROM (SELECT id,name FROM guard_order)", fields: []string{"name"}, order: "1 DESC", want: []any{"z"}},
		{name: "schema table wildcard", source: "SELECT guard_order.* FROM main.guard_order", columns: []*data.Column{{Name: "id"}, {Name: "name"}}, fields: []string{"name"}, order: "1 DESC", want: []any{"z"}},
		{name: "selected quoted alias", source: "SELECT id,u.name AS `display_name` FROM guard_order u", fields: []string{"display_name"}, order: "display_name DESC", want: []any{"z"}},
		{name: "omitted alias uses source expression", source: "SELECT id,u.name AS display_name FROM guard_order u", fields: []string{"id"}, order: "display_name DESC", wantSQL: "SELECT id FROM guard_order u ORDER BY u.name DESC", want: []any{int64(1)}},
		{name: "omitted constant alias", source: "SELECT id,7 AS extra FROM guard_order", fields: []string{"id"}, order: "extra", want: []any{int64(1)}},
		{name: "star request order", source: "SELECT * FROM guard_order", columns: []*data.Column{{Name: "id"}, {Name: "name"}, {Name: "secret"}}, fields: []string{"name", "id"}, order: "1 DESC", want: []any{"z", int64(1)}},
		{name: "star omitted order retains outer scope", source: "SELECT * FROM guard_order", columns: []*data.Column{{Name: "id"}, {Name: "name"}, {Name: "secret"}}, fields: []string{"id"}, order: "name DESC", want: []any{int64(1)}},
		{name: "star ordinal matches emitted metadata order", source: "SELECT * FROM guard_order", columns: []*data.Column{{Name: "name"}, {Name: "id"}}, order: "1 DESC", allowed: []spec.FieldPath{"name"}, wantSQL: "SELECT guard_order.name, guard_order.id FROM guard_order ORDER BY 1 DESC", want: []any{"z", int64(1)}},
		{name: "EXCEPT selected executes", source: "SELECT u.* EXCEPT(secret) FROM guard_order u", columns: []*data.Column{{Name: "id"}, {Name: "name"}, {Name: "secret"}}, fields: []string{"name"}, order: "1 DESC", want: []any{"z"}},
		{name: "EXCEPT order executes", source: "SELECT u.* EXCEPT(secret) FROM guard_order u", columns: []*data.Column{{Name: "id"}, {Name: "name"}, {Name: "secret"}}, order: "2 DESC", want: []any{int64(1), "z"}},
		{name: "CTE criteria marker", source: "WITH a AS (SELECT id,name FROM guard_order) SELECT id,name FROM a $WHERE_CRITERIA", fields: []string{"name"}, order: "1 DESC", want: []any{"z"}},
		{name: "set alias narrowed", source: "SELECT id,name AS label FROM guard_order UNION ALL SELECT id,name AS label FROM guard_order", fields: []string{"label"}, order: "1 DESC", want: []any{"z"}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			opts := []BuilderOption{WithBuilderView(&data.View{Columns: tc.columns}), WithBuilderSQL(tc.source), WithBuilderProjection(tc.fields), WithBuilderSelector(&xstate.Selector{OrderBy: tc.order}), WithBuilderSelectorPolicy(&spec.Selector{AllowFields: true, AllowOrderBy: true, Orderable: tc.allowed})}
			q, err := NewBuilder().Build(ctx, opts...)
			require.NoError(t, err)
			if tc.wantSQL != "" {
				require.Equal(t, tc.wantSQL, q.SQL)
			}
			rows, err := h.DB.QueryContext(ctx, q.SQL, q.Args...)
			require.NoError(t, err, q.SQL)
			defer rows.Close()
			require.True(t, rows.Next())
			values := make([]any, len(tc.want))
			dest := make([]any, len(values))
			for i := range values {
				dest[i] = &values[i]
			}
			require.NoError(t, rows.Scan(dest...))
			require.Equal(t, tc.want, values)
		})
	}
}

func TestSourceGuardOrderAliasesAndOrdinalPolicy(t *testing.T) {
	for _, tc := range []struct {
		name, order string
		fields      []string
		aliases     map[string]spec.FieldPath
		allowed     []spec.FieldPath
		reject      string
	}{
		{name: "native alias vocabulary", order: "DISPLAY_NAME DESC", aliases: map[string]spec.FieldPath{"display_name": "name"}},
		{name: "alias cannot widen", order: "display_name", aliases: map[string]spec.FieldPath{"display_name": "secret"}, reject: "source projection"},
		{name: "conflicting canonical aliases", order: "display_name", aliases: map[string]spec.FieldPath{"display_name": "name", "Display_Name": "id"}, reject: "ambiguous order alias"},
		{name: "ordinal uses source order", order: "1", fields: []string{"name", "id"}, allowed: []spec.FieldPath{"id"}},
		{name: "ordinal respects restriction", order: "2", fields: []string{"name", "id"}, allowed: []spec.FieldPath{"id"}, reject: "not allowed"},
		{name: "negative ordinal", order: "-1", reject: "outside source projection"},
		{name: "overflow ordinal", order: "999999999999999999999999999999", reject: "invalid order by position"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			_, err := NewBuilder().Build(context.Background(), WithBuilderSQL("SELECT id,name FROM users"), WithBuilderProjection(tc.fields), WithBuilderSelector(&xstate.Selector{OrderBy: tc.order}), WithBuilderSelectorPolicy(&spec.Selector{AllowFields: true, AllowOrderBy: true, Orderable: tc.allowed, OrderAliases: tc.aliases}))
			if tc.reject != "" {
				require.ErrorContains(t, err, tc.reject)
			} else {
				require.NoError(t, err)
			}
		})
	}
}

func TestSourceGuardTableOrderingAcrossBuilderPaths(t *testing.T) {
	ctx := context.Background()
	view := &data.View{Spec: spec.View{Source: &spec.ViewSource{Table: "users"}, Selector: &spec.Selector{AllowOrderBy: true}}, Columns: []*data.Column{{Name: "UserID", Column: "user_id"}}}
	opts := []BuilderOption{WithBuilderView(view), WithBuilderSelector(&xstate.Selector{OrderBy: "user_id DESC"})}
	b := NewBuilder()
	q, err := b.Build(ctx, opts...)
	require.NoError(t, err)
	require.Contains(t, q.SQL, "ORDER BY user_id DESC")
	_, err = b.CacheSQL(ctx, opts...)
	require.NoError(t, err)
	_, err = b.QueryMatcher(ctx, q, opts...)
	require.NoError(t, err)
}
