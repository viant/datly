package builder

import (
	"context"
	"github.com/stretchr/testify/require"
	"github.com/viant/datly/data"
	"github.com/viant/datly/internal/testharness/sqlite"
	"testing"
)

func TestRelationFilterPreservesLineCommentBoundary(t *testing.T) {
	ctx := context.Background()
	db := sqlite.New(t)
	require.NoError(t, db.ExecStatements(ctx, "CREATE TABLE bids(campaign_id INTEGER,bid_count INTEGER)", "INSERT INTO bids VALUES(1,2),(1,3),(2,100)"))
	relation := &data.Relation{Of: &data.RelationRef{On: data.Links{{Column: "campaign_id"}}}}
	for _, source := range []string{
		"SELECT campaign_id,SUM(bid_count) AS total FROM bids WHERE 1=1\n-- disabled predicate\nGROUP BY campaign_id",
		"SELECT campaign_id,SUM(bid_count) AS total FROM bids WHERE 1=1\r\n-- disabled predicate\r\nGROUP BY campaign_id",
		"SELECT campaign_id,bid_count AS total FROM bids WHERE 1=1 -- disabled predicate",
	} {
		t.Run(source, func(t *testing.T) {
			q, err := NewBuilder().Build(ctx, WithBuilderSQL(source), WithBuilderRelation(relation), WithBuilderPositionalArgs([]any{1}))
			require.NoError(t, err)
			require.Equal(t, []any{1}, q.Args)
			rows, err := db.DB.QueryContext(ctx, q.SQL, q.Args...)
			require.NoError(t, err)
			defer rows.Close()
			sum := 0
			for rows.Next() {
				var campaign, total int
				require.NoError(t, rows.Scan(&campaign, &total))
				require.Equal(t, 1, campaign)
				sum += total
			}
			require.NoError(t, rows.Err())
			require.Equal(t, 5, sum)
		})
	}
}
