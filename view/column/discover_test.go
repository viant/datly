package column

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestParseQuery_WithCTEStar_DoesNotShortCircuitToTableMetadata(t *testing.T) {
	sql := `WITH cte AS (SELECT 1 AS a) SELECT v.* FROM cte v`

	table, discoveredSQL, cols := parseQuery(sql)
	require.Equal(t, "cte", strings.TrimSpace(table))
	require.NotEmpty(t, cols)
	require.NotEmpty(t, strings.TrimSpace(discoveredSQL), "CTE star query must keep SQL for runtime column inference")
	require.Contains(t, strings.ToUpper(discoveredSQL), "WITH CTE AS")
	require.Contains(t, discoveredSQL, "LIMIT 1")
}

func TestParseQuery_WithCaseExpression_DoesNotPanic(t *testing.T) {
	table, discoveredSQL, cols := parseQuery(
		`SELECT CASE WHEN status_flag = 1 THEN 'active' ELSE 'inactive' END AS status_bucket FROM account_window`,
	)
	require.Equal(t, "account_window", strings.TrimSpace(table))
	require.Len(t, cols, 0)
	require.Equal(t, "", discoveredSQL)
}
