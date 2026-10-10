package builder

import (
	"context"
	"github.com/stretchr/testify/require"
	"github.com/viant/datly/data"
	"github.com/viant/datly/internal/testharness/sqlite"
	"github.com/viant/datly/spec"
	"github.com/viant/sqlx/io/read/cache"
	"strings"
	"testing"
)

func TestOriginalPartitionInnerNamespaceSQLite(t *testing.T) {
	ctx := context.Background()
	db := sqlite.New(t)
	require.NoError(t, db.ExecStatements(ctx, "CREATE TABLE audience(ID INTEGER,target TEXT,tenant INTEGER)", "INSERT INTO audience VALUES(1,'a',7),(2,'b',7),(3,'c',7),(4,'d',8)"))
	for _, mode := range []string{"Build", "ShapeBound"} {
		t.Run(mode, func(t *testing.T) {
			source := &spec.ViewSource{SQL: "SELECT audience.* FROM (SELECT au.ID,au.target FROM audience au WHERE au.tenant = ?) audience WHERE audience.ID < ?"}
			original := source.SQL
			view := &data.View{Spec: spec.View{Source: source}, Columns: []*data.Column{{Name: "ID", Column: "ID"}, {Name: "target", Column: "target"}}}
			partition := &PartitionInput{Expression: " au.ID BETWEEN ? AND ?", Args: []any{2, 3}}
			options := []BuilderOption{WithBuilderView(view), WithBuilderPartition(partition)}
			var q *cache.ParmetrizedQuery
			var err error
			if mode == "Build" {
				q, err = NewBuilder().Build(ctx, append(options, WithBuilderPositionalArgs([]any{7, 4}))...)
			} else {
				q, err = NewBuilder().ShapeBound(&cache.ParmetrizedQuery{SQL: source.SQL, Args: []any{7, 4}}, options...)
			}
			require.NoError(t, err)
			require.Equal(t, []any{7, 2, 3, 4}, q.Args, q.SQL)
			rows, err := db.DB.QueryContext(ctx, q.SQL, q.Args...)
			require.NoError(t, err, q.SQL)
			defer rows.Close()
			var ids []int
			for rows.Next() {
				var id int
				var target string
				require.NoError(t, rows.Scan(&id, &target))
				ids = append(ids, id)
			}
			require.NoError(t, rows.Err())
			require.Equal(t, []int{2, 3}, ids)
			require.Equal(t, original, source.SQL)
			require.Equal(t, " au.ID BETWEEN ? AND ?", partition.Expression)
		})
	}
}

func TestPartitionCriteriaDeclaredScope(t *testing.T) {
	for _, tc := range []struct {
		name, source, expression, scope string
		invalid                         bool
	}{
		{name: "outer alias", source: "SELECT audience.* FROM (SELECT au.ID FROM audience au) audience WHERE audience.ID>0", expression: "audience.ID BETWEEN ? AND ?", scope: "SELECT audience.*"},
		{name: "inner joined alias", source: "SELECT audience.* FROM (SELECT au.ID FROM audience au JOIN orders ao ON ao.ID=au.ID WHERE au.ID>0) audience", expression: "ao.ID BETWEEN ? AND ?", scope: "SELECT au.ID"},
		{name: "nested wrappers", source: "SELECT n.* FROM (SELECT audience.* FROM (SELECT au.ID FROM audience au) audience) n", expression: "au.ID BETWEEN ? AND ?", scope: "SELECT au.ID"},
		{name: "quoted namespace", source: "SELECT audience.* FROM (SELECT `au`.ID FROM audience `au`) audience", expression: "`au`.ID BETWEEN ? AND ?", scope: "SELECT `au`.ID"},
		{name: "alias shadowing", source: "SELECT au.* FROM (SELECT au.ID FROM audience au) au", expression: "au.ID BETWEEN ? AND ?", scope: "SELECT au.*"},
		{name: "unqualified preserves outer", source: "SELECT audience.* FROM (SELECT au.ID FROM audience au) audience", expression: "ID BETWEEN ? AND ?", scope: "SELECT audience.*"},
		{name: "literal namespace ignored", source: "SELECT audience.* FROM (SELECT au.ID FROM audience au) audience", expression: "audience.ID BETWEEN ? AND ? AND 'au.ID'='au.ID'", scope: "SELECT audience.*"},
		{name: "comment source ignored", source: "SELECT audience.* FROM (SELECT au.ID FROM audience au) audience /* (SELECT au.ID FROM audience au) */", expression: "au.ID BETWEEN ? AND ?", scope: "SELECT au.ID"},
		{name: "functions", source: "SELECT audience.* FROM (SELECT au.ID FROM audience au) audience", expression: "ABS(au.ID) BETWEEN ? AND ?", scope: "SELECT au.ID"},
		{name: "mixed scopes rejected", source: "SELECT audience.* FROM (SELECT au.ID FROM audience au) audience", expression: "au.ID BETWEEN ? AND ? AND audience.ID>0", invalid: true},
		{name: "unknown namespace rejected", source: "SELECT audience.* FROM (SELECT au.ID FROM audience au) audience", expression: "wrong.ID BETWEEN ? AND ?", invalid: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			begin, end, err := partitionCriteriaScope(tc.source, tc.expression)
			if tc.invalid {
				require.Error(t, err)
				return
			}
			require.NoError(t, err)
			require.Contains(t, tc.source[begin:end], tc.scope)
			require.Equal(t, strings.Index(tc.source, tc.scope), begin)
		})
	}
}
