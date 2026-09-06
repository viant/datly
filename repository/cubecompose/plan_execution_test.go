package cubecompose

import (
	"database/sql"
	"testing"

	_ "github.com/mattn/go-sqlite3"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestPlanRender_ExecutesPortableTopDropQuery(t *testing.T) {
	plan, err := Compile(`SELECT t1.ad_order_id,
 COALESCE(t2.total_spend, 0) AS current_spend,
 t1.total_spend AS previous_spend,
 t1.total_spend - COALESCE(t2.total_spend, 0) AS spend_drop
 FROM $CubeSQL1 AS t1
 LEFT JOIN $CubeSQL2 AS t2 ON t1.ad_order_id = t2.ad_order_id
 WHERE COALESCE(t2.total_spend, 0) < t1.total_spend
 ORDER BY spend_drop DESC
 LIMIT 2`, testCatalog(t), 100)
	require.NoError(t, err)

	previous := `SELECT * FROM (
 SELECT 1 AS ad_order_id, 100.0 AS total_spend
 UNION ALL SELECT 2, 80.0
 UNION ALL SELECT 3, 30.0
) previous_spend WHERE ? = 29 AND ? = '2026-09-04' AND ? = '2026-09-05'`
	current := `SELECT * FROM (
 SELECT 1 AS ad_order_id, 40.0 AS total_spend
 UNION ALL SELECT 3, 25.0
) current_spend WHERE ? = 29 AND ? = '2026-09-05' AND ? = '2026-09-06'`
	query, args, err := plan.Render(
		previous,
		current,
		[]interface{}{29, "2026-09-04", "2026-09-05"},
		[]interface{}{29, "2026-09-05", "2026-09-06"},
	)
	require.NoError(t, err)
	assert.Equal(t, []interface{}{
		int64(0), int64(0),
		29, "2026-09-04", "2026-09-05",
		29, "2026-09-05", "2026-09-06",
		int64(0),
	}, args)

	db, err := sql.Open("sqlite3", ":memory:")
	require.NoError(t, err)
	defer db.Close()
	rows, err := db.Query(query, args...)
	require.NoError(t, err)
	defer rows.Close()

	type drop struct {
		orderID  int
		current  float64
		previous float64
		delta    float64
	}
	var actual []drop
	for rows.Next() {
		item := drop{}
		require.NoError(t, rows.Scan(&item.orderID, &item.current, &item.previous, &item.delta))
		actual = append(actual, item)
	}
	require.NoError(t, rows.Err())
	assert.Equal(t, []drop{
		{orderID: 2, current: 0, previous: 80, delta: 80},
		{orderID: 1, current: 40, previous: 100, delta: 60},
	}, actual)
}
