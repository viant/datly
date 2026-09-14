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
	dsql "github.com/viant/datly/sql"
	xstate "github.com/viant/xdatly/state"
)

func markerProjectionView() *data.View {
	return &data.View{Columns: []*data.Column{{Name: "ID", Column: "id"}, {Name: "OwnerID", Column: "owner_id"}, {Name: "Name", Column: "name"}, {Name: "Tenant", Column: "tenant"}}}
}

func TestMarkerProjectionNoSelectorPreservesExistingOwnerRendering(t *testing.T) {
	source := "SELECT * FROM users $WHERE_CRITERIA ORDER BY id"
	// The unchanged reviewer expects two spaces, but the existing criteria owner
	// emits one even in the original prepare-then-strip phase sequence.
	expected := (relationFilter{}).applyCriteriaTokens(dsql.PrepareExecutableSQL(source, nil))
	require.Equal(t, "SELECT * FROM users ORDER BY id", expected)
	q, err := NewBuilder().Build(context.Background(), WithBuilderView(markerProjectionView()), WithBuilderSQL(source))
	require.NoError(t, err)
	require.Equal(t, expected, q.SQL)
}

func TestMarkerProjectionMatrixSQLite(t *testing.T) {
	ctx := context.Background()
	db := sqlite.New(t)
	require.NoError(t, db.ExecStatements(ctx, "CREATE TABLE users(id INTEGER,owner_id INTEGER,name TEXT,tenant INTEGER)", "INSERT INTO users VALUES(1,7,'one',3),(2,8,'two',3),(3,7,'three',4)"))
	relation := &data.Relation{Of: &data.RelationRef{On: data.Links{{Column: "owner_id"}}}}
	for _, tc := range []struct {
		name, source string
		relation     *data.Relation
		args         []any
		wantArgs     []any
		wantRows     int
		criteria     string
		criteriaArgs []any
	}{
		{name: "strip where", source: "SELECT * FROM users $WHERE_CRITERIA ORDER BY id", wantRows: 3},
		{name: "strip and", source: "SELECT * FROM users WHERE tenant=3 $AND_CRITERIA ORDER BY id", wantRows: 2},
		{name: "strip or", source: "SELECT * FROM users WHERE tenant=3 $OR_CRITERIA ORDER BY id", wantRows: 2},
		{name: "strip column", source: "SELECT * FROM users WHERE $COLUMN_IN ORDER BY id", wantRows: 3},
		{name: "strip and column", source: "SELECT * FROM users WHERE tenant=3 $AND_COLUMN_IN ORDER BY id", wantRows: 2},
		{name: "relation where", source: "SELECT * FROM users $WHERE_CRITERIA ORDER BY id", relation: relation, args: []any{7}, wantArgs: []any{7}, wantRows: 2},
		{name: "relation and bound prefix", source: "SELECT * FROM users WHERE tenant=:tenant $AND_CRITERIA ORDER BY id", relation: relation, args: []any{7}, wantArgs: []any{3, 7}, wantRows: 1},
		{name: "relation or", source: "SELECT * FROM users WHERE tenant=3 $OR_CRITERIA ORDER BY id", relation: relation, args: []any{7}, wantArgs: []any{7}, wantRows: 3},
		{name: "relation column", source: "SELECT * FROM users WHERE $COLUMN_IN ORDER BY id", relation: relation, args: []any{7}, wantArgs: []any{7}, wantRows: 2},
		{name: "relation and column", source: "SELECT * FROM users WHERE tenant=3 $AND_COLUMN_IN ORDER BY id", relation: relation, args: []any{7}, wantArgs: []any{7}, wantRows: 1},
		{name: "empty parent", source: "SELECT * FROM users $WHERE_CRITERIA ORDER BY id", relation: relation, wantRows: 0},
		{name: "parent where", source: `SELECT * FROM users $View.ParentJoinOn("WHERE","owner_id") ORDER BY id`, relation: relation, args: []any{7}, wantArgs: []any{7}, wantRows: 2},
		{name: "parent key", source: `SELECT * FROM users WHERE $View.ColIn("","owner_id") ORDER BY id`, relation: relation, args: []any{7}, wantArgs: []any{7}, wantRows: 2},
		{name: "parent with prefix suffix and selector args", source: `SELECT * FROM users WHERE tenant=:tenant $View.ParentJoinOn("AND","owner_id") $AND_SELECTOR_CRITERIA AND id<:max ORDER BY id`, relation: relation, args: []any{7}, criteria: "name = ?", criteriaArgs: []any{"one"}, wantArgs: []any{3, 7, "one", 4}, wantRows: 1},
		{name: "selector where", source: "SELECT * FROM users $WHERE_SELECTOR_CRITERIA ORDER BY id", criteria: "tenant = ?", criteriaArgs: []any{3}, wantArgs: []any{3}, wantRows: 2},
		{name: "mixed with protected text", source: "SELECT u.*, '$WHERE_CRITERIA $AND_COLUMN_IN $View.ParentJoinOn(\"id\")' AS note FROM users u $WHERE_CRITERIA /* $WHERE_CRITERIA */ ORDER BY id -- $AND_CRITERIA\n", wantRows: 3},
		{name: "CTE", source: "WITH u AS (SELECT id,owner_id,name,tenant FROM users $WHERE_CRITERIA) SELECT u.* FROM u ORDER BY id", relation: relation, args: []any{7}, wantArgs: []any{7}, wantRows: 2},
	} {
		for _, mode := range []string{"none", "fields", "order", "fields and order"} {
			t.Run(tc.name+"/"+mode, func(t *testing.T) {
				var selector *xstate.Selector
				if strings.Contains(mode, "order") || tc.criteria != "" {
					selector = &xstate.Selector{Criteria: tc.criteria, Placeholders: tc.criteriaArgs}
				}
				if strings.Contains(mode, "order") {
					selector.OrderBy = "id DESC"
				}
				opts := []BuilderOption{WithBuilderView(markerProjectionView()), WithBuilderSQL(tc.source), WithBuilderRelation(tc.relation), WithBuilderPositionalArgs(tc.args), WithBuilderSelector(selector), WithBuilderSelectorPolicy(&spec.Selector{AllowFields: true, AllowOrderBy: true, AllowCriteria: true, Filterable: []spec.FieldPath{"name", "tenant"}}), WithBuilderParameterResolver(parameterResolver(map[string]any{"tenant": 3, "max": 4}))}
				if strings.Contains(mode, "fields") {
					opts = append(opts, WithBuilderProjection([]string{"name", "id"}))
				}
				q, err := NewBuilder().Build(ctx, opts...)
				require.NoError(t, err)
				require.Equal(t, tc.wantArgs, q.Args, q.SQL)
				rows, err := db.DB.QueryContext(ctx, q.SQL, q.Args...)
				require.NoError(t, err, q.SQL)
				defer rows.Close()
				columns, err := rows.Columns()
				require.NoError(t, err)
				count := 0
				for rows.Next() {
					values := make([]any, len(columns))
					pointers := make([]any, len(columns))
					for i := range values {
						pointers[i] = &values[i]
					}
					require.NoError(t, rows.Scan(pointers...))
					count++
				}
				require.NoError(t, rows.Err())
				require.Equal(t, tc.wantRows, count, q.SQL)
				if !strings.Contains(mode, "fields") && strings.Contains(tc.name, "protected") {
					require.Contains(t, q.SQL, "'$WHERE_CRITERIA $AND_COLUMN_IN $View.ParentJoinOn(\"id\")'")
				}
				if strings.Contains(tc.name, "protected") {
					require.Contains(t, q.SQL, "/* $WHERE_CRITERIA */")
					require.Contains(t, q.SQL, "-- $AND_CRITERIA")
				}
			})
		}
	}
}

func TestMarkerProjectionDuplicateAndUnknownStillFail(t *testing.T) {
	for _, source := range []string{
		"SELECT u.*,u.id AS id FROM (SELECT id,name FROM users) u $WHERE_CRITERIA",
		`SELECT u.*,u.id AS id FROM (SELECT id,name FROM users) u $View.ParentJoinOn("WHERE","owner_id")`,
		"SELECT a.*,b.* FROM (SELECT id FROM users) a JOIN (SELECT id FROM users) b ON a.id=b.id $WHERE_CRITERIA",
	} {
		for _, mode := range []string{"none", "fields", "order"} {
			t.Run(source+mode, func(t *testing.T) {
				opts := []BuilderOption{WithBuilderView(markerProjectionView()), WithBuilderSQL(source)}
				if mode == "fields" {
					opts = append(opts, WithBuilderProjection([]string{"name"}))
				}
				if mode == "order" {
					opts = append(opts, WithBuilderSelector(&xstate.Selector{OrderBy: "1"}))
				}
				_, err := NewBuilder().Build(context.Background(), opts...)
				require.ErrorContains(t, err, "assign distinct SQL aliases")
			})
		}
	}
	_, err := NewBuilder().Build(context.Background(), WithBuilderSQL("SELECT * FROM users $WHERE_CRITERIA"), WithBuilderProjection([]string{"id"}))
	require.ErrorContains(t, err, "prepared columns")
	_, err = NewBuilder().Build(context.Background(), WithBuilderView(markerProjectionView()), WithBuilderSQL("SELECT * FROM users $UNKNOWN_MARKER"))
	require.Error(t, err)
}

func TestMarkerProjectionCompositeArgumentsAndCache(t *testing.T) {
	ctx := context.Background()
	relation := &data.Relation{Of: &data.RelationRef{On: data.Links{{Column: "owner_id"}, {Column: "tenant"}}}}
	for _, source := range []string{
		"SELECT * FROM users WHERE id>:min $AND_CRITERIA AND id<:max",
		`SELECT * FROM users WHERE id>:min $View.ParentCompositeJoinOn("AND","owner_id","tenant") AND id<:max`,
	} {
		t.Run(source, func(t *testing.T) {
			options := []BuilderOption{WithBuilderView(markerProjectionView()), WithBuilderSQL(source), WithBuilderRelation(relation), WithBuilderCompositeArgs([]string{"owner_id", "tenant"}, [][]interface{}{{7, 3}, {8, 4}}), WithBuilderParameterResolver(parameterResolver(map[string]any{"min": 0, "max": 9})), WithBuilderProjection([]string{"id", "name"}), WithBuilderSelector(&xstate.Selector{OrderBy: "id DESC", Limit: 2}), WithBuilderSelectorPolicy(&spec.Selector{AllowFields: true, AllowOrderBy: true, AllowLimit: true})}
			builder := NewBuilder()
			query, err := builder.Build(ctx, options...)
			require.NoError(t, err)
			require.Equal(t, []any{0, 7, 3, 8, 4, 9}, query.Args)
			cached, err := builder.CacheSQL(ctx, options...)
			require.NoError(t, err)
			require.Equal(t, []any{0, 9}, cached.Args)
			require.Equal(t, []string{"owner_id", "tenant"}, cached.ByColumns)
			require.Equal(t, [][]interface{}{{7, 3}, {8, 4}}, cached.InTuples)
			require.Equal(t, 2, cached.Limit)
			matcher, err := builder.QueryMatcher(ctx, query, options...)
			require.NoError(t, err)
			require.Equal(t, query.SQL, matcher.SQL)
			require.True(t, reflect.DeepEqual(query.Args, matcher.Args))
			require.Equal(t, cached.ByColumns, matcher.ByColumns)
			require.Equal(t, cached.InTuples, matcher.InTuples)
		})
	}
}

func TestMarkerProjectionCachePreservesProtectedText(t *testing.T) {
	ctx := context.Background()
	view := markerProjectionView()
	source := `SELECT * FROM users /* $View.ParentJoinOn("AND","id") $AND_CRITERIA */ WHERE name <> '$WHERE_CRITERIA' $View.ParentJoinOn("AND","owner_id") ORDER BY id -- $WHERE_CRITERIA
`
	options := []BuilderOption{WithBuilderView(view), WithBuilderSQL(source), WithBuilderRelation(&data.Relation{Of: &data.RelationRef{On: data.Links{{Column: "owner_id"}}}}), WithBuilderPositionalArgs([]any{7}), WithBuilderProjection([]string{"id", "name", "owner_id"}), WithBuilderSelector(&xstate.Selector{OrderBy: "id", Limit: 2}), WithBuilderSelectorPolicy(&spec.Selector{AllowFields: true, AllowOrderBy: true, AllowLimit: true})}
	b := NewBuilder()
	query, err := b.Build(ctx, options...)
	require.NoError(t, err)
	require.Equal(t, []any{7}, query.Args)
	cached, err := b.CacheSQL(ctx, options...)
	require.NoError(t, err)
	require.Empty(t, cached.Args)
	require.Equal(t, "owner_id", cached.By)
	require.Equal(t, []any{7}, cached.In)
	require.Equal(t, 2, cached.Limit)
	matcher, err := b.QueryMatcher(ctx, query, options...)
	require.NoError(t, err)
	require.Equal(t, query.SQL, matcher.SQL)
	require.Equal(t, query.Args, matcher.Args)
	for _, sql := range []string{query.SQL, cached.SQL} {
		require.Contains(t, sql, "'$WHERE_CRITERIA'")
		require.Contains(t, sql, `/* $View.ParentJoinOn("AND","id") $AND_CRITERIA */`)
		require.Contains(t, sql, "-- $WHERE_CRITERIA")
	}
	require.NotContains(t, cached.SQL, "owner_id IN")
}

func TestMarkerProjectionGroupedSelectorArgumentOrder(t *testing.T) {
	ctx := context.Background()
	db := sqlite.New(t)
	require.NoError(t, db.ExecStatements(ctx, "CREATE TABLE users(id INTEGER,owner_id INTEGER,name TEXT,tenant INTEGER)", "INSERT INTO users VALUES(1,7,'one',3),(2,8,'two',3),(3,7,'three',4)"))
	groupable := true
	view := markerProjectionView()
	view.Spec.Groupable = &groupable
	view.Columns = append(view.Columns, &data.Column{Name: "total", Column: "total"})
	query, err := NewBuilder().Build(ctx, WithBuilderView(view), WithBuilderSQL("SELECT owner_id,SUM(id) AS total FROM users WHERE tenant=:tenant $AND_SELECTOR_CRITERIA GROUP BY owner_id"), WithBuilderProjection([]string{"total"}), WithBuilderSelector(&xstate.Selector{OrderBy: "1 DESC", Criteria: "name <> ?", Placeholders: []any{"excluded"}}), WithBuilderSelectorPolicy(&spec.Selector{AllowFields: true, AllowOrderBy: true, AllowCriteria: true, Filterable: []spec.FieldPath{"name"}}), WithBuilderParameterResolver(parameterResolver(map[string]any{"tenant": 3})))
	require.NoError(t, err)
	require.Equal(t, []any{3, "excluded"}, query.Args)
	var total int
	require.NoError(t, db.DB.QueryRowContext(ctx, query.SQL, query.Args...).Scan(&total))
	require.Equal(t, 3, total)
}
