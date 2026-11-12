package translator

import (
	"context"
	"database/sql"
	"path/filepath"
	"testing"

	_ "github.com/mattn/go-sqlite3"
	"github.com/stretchr/testify/require"
)

func TestViewlet_discoverTables_GroupableColumnConfig(t *testing.T) {
	ctx := context.Background()
	dsn := filepath.Join(t.TempDir(), "viewlet_groupable.sqlite")
	db, err := sql.Open("sqlite3", dsn)
	require.NoError(t, err)
	t.Cleanup(func() { _ = db.Close() })

	_, err = db.ExecContext(ctx, `CREATE TABLE sales (region_id TEXT, total_sales REAL, country_id TEXT)`)
	require.NoError(t, err)

	useCases := []struct {
		description string
		sql         string
		groupable   bool
		expect      map[string]bool
	}{
		{
			description: "flags groupable columns from ordinal group by",
			sql:         `SELECT region_id, SUM(total_sales) AS total_sales, country_id FROM sales GROUP BY 1, 3`,
			groupable:   true,
			expect: map[string]bool{
				"region_id":  true,
				"country_id": true,
			},
		},
		{
			description: "flags groupable columns from alias and name group by",
			sql:         `SELECT region_id AS region, SUM(total_sales) AS total_sales, country_id FROM sales GROUP BY region, country_id`,
			groupable:   true,
			expect: map[string]bool{
				"region":     true,
				"country_id": true,
			},
		},
		{
			description: "does not infer groupable columns without explicit view grouping",
			sql:         `SELECT region_id, SUM(total_sales) AS total_sales, country_id FROM sales GROUP BY 1, 3`,
			groupable:   false,
			expect:      map[string]bool{},
		},
	}

	for _, useCase := range useCases {
		t.Run(useCase.description, func(t *testing.T) {
			viewlet := NewViewlet("sales", useCase.sql, nil, &Resource{})
			viewlet.View = &View{}
			viewlet.View.Groupable = useCase.groupable
			err := viewlet.discoverTables(ctx, db, useCase.sql)
			require.NoError(t, err)

			actual := map[string]bool{}
			for _, config := range viewlet.ColumnConfig {
				require.NotNil(t, config.Groupable)
				actual[config.Name] = *config.Groupable
			}
			require.Equal(t, useCase.expect, actual)
		})
	}
}
