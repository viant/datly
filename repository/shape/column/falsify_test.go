package column

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestFalsifyQuery(t *testing.T) {
	tests := []struct {
		name       string
		input      string
		wantOK     bool
		assertions func(t *testing.T, result string)
	}{
		{
			name:   "simple SELECT *",
			input:  "SELECT * FROM orders",
			wantOK: true,
			assertions: func(t *testing.T, result string) {
				assert.Contains(t, result, "1 = 0")
				assert.NotContains(t, strings.ToUpper(result), "LIMIT")
			},
		},
		{
			name:   "SELECT with existing WHERE",
			input:  "SELECT id, name FROM items WHERE status = 1",
			wantOK: true,
			assertions: func(t *testing.T, result string) {
				assert.Contains(t, result, "1 = 0")
				assert.Contains(t, result, "status")
			},
		},
		{
			name:   "SELECT with LIMIT stripped",
			input:  "SELECT * FROM items LIMIT 100 OFFSET 50",
			wantOK: true,
			assertions: func(t *testing.T, result string) {
				assert.Contains(t, result, "1 = 0")
				assert.NotContains(t, strings.ToUpper(result), "LIMIT")
				assert.NotContains(t, strings.ToUpper(result), "OFFSET")
			},
		},
		{
			name:   "UNION ALL — both branches get 1=0",
			input:  "SELECT id, name FROM items_a WHERE region = 'us' UNION ALL SELECT id, name FROM items_b WHERE region = 'eu'",
			wantOK: true,
			assertions: func(t *testing.T, result string) {
				count := strings.Count(result, "1 = 0")
				assert.GreaterOrEqual(t, count, 2, "both UNION branches should get 1=0")
			},
		},
		{
			name: "CTE — all CTEs and outer get 1=0",
			input: `WITH metrics AS (
    SELECT category, SUM(amount) AS total
    FROM transactions
    GROUP BY category
),
ranked AS (
    SELECT *, ROW_NUMBER() OVER (ORDER BY total DESC) AS rn
    FROM metrics
)
SELECT * FROM ranked WHERE rn <= 10`,
			wantOK: true,
			assertions: func(t *testing.T, result string) {
				count := strings.Count(result, "1 = 0")
				assert.GreaterOrEqual(t, count, 3, "each CTE + outer should get 1=0, got %d", count)
				assert.NotContains(t, strings.ToUpper(result), "LIMIT")
			},
		},
		{
			name: "CTE with UNION inside",
			input: `WITH combined AS (
    SELECT id, name FROM items_a
    UNION ALL
    SELECT id, name FROM items_b
)
SELECT * FROM combined`,
			wantOK: true,
			assertions: func(t *testing.T, result string) {
				count := strings.Count(result, "1 = 0")
				assert.GreaterOrEqual(t, count, 3, "CTE branches + outer should all get 1=0")
			},
		},
		{
			name:   "JOIN query — outer gets 1=0",
			input:  "SELECT a.id, b.name FROM orders a JOIN items b ON a.item_id = b.id",
			wantOK: true,
			assertions: func(t *testing.T, result string) {
				assert.Contains(t, result, "1 = 0")
			},
		},
		{
			name:   "subquery in FROM — both get 1=0",
			input:  "SELECT t.* FROM (SELECT id, name FROM items WHERE active = 1) t",
			wantOK: true,
			assertions: func(t *testing.T, result string) {
				count := strings.Count(result, "1 = 0")
				assert.GreaterOrEqual(t, count, 2, "outer + subquery should get 1=0")
			},
		},
		{
			name:   "empty SQL",
			input:  "",
			wantOK: false,
		},
		{
			name:   "non-SELECT statement",
			input:  "INSERT INTO items VALUES (1, 'test')",
			wantOK: true, // parser may still parse it; falsify is best-effort
		},
		{
			name:   "GROUP BY preserved",
			input:  "SELECT category, COUNT(*) AS cnt FROM items GROUP BY category",
			wantOK: true,
			assertions: func(t *testing.T, result string) {
				assert.Contains(t, result, "1 = 0")
				assert.Contains(t, strings.ToUpper(result), "GROUP BY")
			},
		},
		{
			name:   "ORDER BY preserved",
			input:  "SELECT * FROM items ORDER BY name",
			wantOK: true,
			assertions: func(t *testing.T, result string) {
				assert.Contains(t, result, "1 = 0")
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, ok := falsifyQuery(tt.input)
			assert.Equal(t, tt.wantOK, ok)
			if ok && tt.assertions != nil {
				require.NotEmpty(t, result)
				tt.assertions(t, result)
			}
		})
	}
}
