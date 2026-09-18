package sql

import (
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/viant/datly/data"
	"github.com/viant/datly/spec"
	"github.com/viant/sqlparser"
)

func TestSupplyPathProjection(t *testing.T) {
	const source = `WITH node_flat AS (
 SELECT sb.xid, node_offset,
   (sb.path.nodes[SAFE_OFFSET(node_offset)]).index AS node_index,
   (sb.path.nodes[SAFE_OFFSET(node_offset)]).sellerDomain AS seller_domain
 FROM bids sb
 CROSS JOIN UNNEST(GENERATE_ARRAY(0, GREATEST(COALESCE(ARRAY_LENGTH(sb.path.nodes), 0) - 1, -1))) AS node_offset
 GROUP BY ALL
), chains AS (
 SELECT xid, ARRAY_AGG(seller_domain IGNORE NULLS ORDER BY node_index ASC) AS domains
 FROM node_flat GROUP BY xid
)
SELECT p.site_id, c.domains[SAFE_OFFSET(GREATEST(ARRAY_LENGTH(c.domains) - 1, 0))] AS seller_domain,
 SUM(p.spend) AS spend_window
FROM performance p JOIN chains c ON p.xid = c.xid
WHERE p.event_date >= ? AND p.order_id = ?
GROUP BY 1, 2 HAVING SUM(p.spend) >= ?`
	on := true
	view := &data.View{Spec: spec.View{Groupable: &on, AllowNulls: &on}}
	for _, tc := range []struct {
		name    string
		columns []string
		groups  int
	}{
		{"full", nil, 2},
		{"site", []string{"site_id", "spend_window"}, 1},
		{"seller", []string{"seller_domain", "spend_window"}, 1},
		{"measure", []string{"spend_window"}, 0},
	} {
		t.Run(tc.name, func(t *testing.T) {
			result, err := ApplySelectorProjection(source, tc.columns, view)
			require.NoError(t, err)
			q, err := sqlparser.ParseQuery(result)
			require.NoError(t, err)
			require.Len(t, q.GroupBy, tc.groups)
			if tc.columns != nil {
				require.Len(t, q.List, len(tc.columns))
				for i, item := range q.List {
					require.Equal(t, tc.columns[i], sqlparser.NewColumn(item).Identity())
				}
			}
			require.Len(t, q.WithSelects, 2)
			require.Contains(t, result, "(sb.path.nodes[SAFE_OFFSET(node_offset)]).sellerDomain")
			require.Contains(t, result, "ARRAY_AGG(seller_domain IGNORE NULLS ORDER BY node_index ASC)")
			require.Contains(t, result, "WHERE p.event_date >= ? AND p.order_id = ?")
			require.Contains(t, result, "HAVING SUM(p.spend) >= ?")
		})
	}
}

func TestAggregateComputedFieldClassification(t *testing.T) {
	q, err := sqlparser.ParseQuery("SELECT (ARRAY_AGG(record IGNORE NULLS ORDER BY id)[SAFE_OFFSET(0)]).name AS name FROM records")
	require.NoError(t, err)
	require.True(t, isAggregateSelectItem(q.List[0]))
	require.Empty(t, groupedProjectionGroupBy(q.List))
}
