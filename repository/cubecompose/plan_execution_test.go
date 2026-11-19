package cubecompose

import (
	"database/sql"
	"fmt"
	"strings"
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
 LIMIT 2`, testCatalog(t), 2, 100)
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
	query, args, err := plan.Render([]Frame{
		{SQL: previous, Args: []interface{}{29, "2026-09-04", "2026-09-05"}},
		{SQL: current, Args: []interface{}{29, "2026-09-05", "2026-09-06"}},
	})
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

func TestPlanRender_ExecutesThreeCubeCompositionWithOrderedBindings(t *testing.T) {
	plan, err := Compile(`SELECT t1.ad_order_id,
 t1.total_spend AS previous_spend,
 COALESCE(t2.total_spend, 0) AS current_spend,
 t3.total_spend AS benchmark_spend,
 t1.total_spend - COALESCE(t2.total_spend, 0) AS spend_drop
 FROM $CubeSQL1 AS t1
 LEFT JOIN $CubeSQL2 AS t2 ON t1.ad_order_id = t2.ad_order_id
 LEFT JOIN $CubeSQL3 AS t3 ON t1.ad_order_id = t3.ad_order_id
 WHERE t3.total_spend > 50
 ORDER BY spend_drop DESC
 LIMIT 5`, testCatalog(t), 3, 100)
	require.NoError(t, err)

	query, args, err := plan.Render([]Frame{
		{
			SQL:  "SELECT 1 AS ad_order_id, 100.0 AS total_spend WHERE ? = 'previous' UNION ALL SELECT 2, 80.0 WHERE ? = 29",
			Args: []interface{}{"previous", 29},
		},
		{
			SQL:  "SELECT 1 AS ad_order_id, 40.0 AS total_spend WHERE ? = 'current' AND ? = 29",
			Args: []interface{}{"current", 29},
		},
		{
			SQL:  "SELECT 1 AS ad_order_id, 90.0 AS total_spend WHERE ? = 'benchmark' UNION ALL SELECT 2, 70.0 WHERE ? = 29",
			Args: []interface{}{"benchmark", 29},
		},
	})
	require.NoError(t, err)
	assert.Equal(t, []interface{}{
		int64(0), int64(0),
		"previous", 29,
		"current", 29,
		"benchmark", 29,
		int64(50),
	}, args)

	db, err := sql.Open("sqlite3", ":memory:")
	require.NoError(t, err)
	defer db.Close()
	rows, err := db.Query(query, args...)
	require.NoError(t, err)
	defer rows.Close()

	type result struct {
		orderID   int
		previous  float64
		current   float64
		benchmark float64
		drop      float64
	}
	var actual []result
	for rows.Next() {
		item := result{}
		require.NoError(t, rows.Scan(&item.orderID, &item.previous, &item.current, &item.benchmark, &item.drop))
		actual = append(actual, item)
	}
	require.NoError(t, rows.Err())
	assert.Equal(t, []result{
		{orderID: 2, previous: 80, current: 0, benchmark: 70, drop: 80},
		{orderID: 1, previous: 100, current: 40, benchmark: 90, drop: 60},
	}, actual)
}

func TestPlanRender_ExecutesTwoThreeAndFourCubesWithDistinctFilterBindings(t *testing.T) {
	for _, frameCount := range []int{2, 3, 4} {
		t.Run(fmt.Sprintf("%d_cubes", frameCount), func(t *testing.T) {
			var composeSQL strings.Builder
			composeSQL.WriteString("SELECT t1.publisher_id")
			for frame := 1; frame <= frameCount; frame++ {
				fmt.Fprintf(&composeSQL, ", t%d.total_spend AS value_%d", frame, frame)
			}
			composeSQL.WriteString(" FROM $CubeSQL1 AS t1")
			for frame := 2; frame <= frameCount; frame++ {
				fmt.Fprintf(&composeSQL, " JOIN $CubeSQL%d AS t%d ON t1.publisher_id = t%d.publisher_id", frame, frame, frame)
			}
			fmt.Fprintf(&composeSQL, " WHERE t%d.total_spend > 0 ORDER BY t1.publisher_id LIMIT 5", frameCount)

			plan, err := Compile(composeSQL.String(), testCatalog(t), frameCount, 100)
			require.NoError(t, err)

			frames := make([]Frame, frameCount)
			var expectedArgs []interface{}
			for frame := 1; frame <= frameCount; frame++ {
				period := fmt.Sprintf("period_%d", frame)
				frameSQL := fmt.Sprintf("SELECT ? AS publisher_id, ? AS total_spend WHERE ? = '%s' AND ? = 101", period)
				frameArgs := []interface{}{101, float64(frame * 10), period, 101}
				frames[frame-1] = Frame{SQL: frameSQL, Args: frameArgs}
				expectedArgs = append(expectedArgs, frameArgs...)
			}
			expectedArgs = append(expectedArgs, int64(0))

			query, args, err := plan.Render(frames)
			require.NoError(t, err)
			assert.Equal(t, expectedArgs, args)

			db, err := sql.Open("sqlite3", ":memory:")
			require.NoError(t, err)
			defer db.Close()
			rows, err := db.Query(query, args...)
			require.NoError(t, err)
			defer rows.Close()

			require.True(t, rows.Next())
			publisherID := 0
			values := make([]float64, frameCount)
			dest := make([]interface{}, 0, frameCount+1)
			dest = append(dest, &publisherID)
			for i := range values {
				dest = append(dest, &values[i])
			}
			require.NoError(t, rows.Scan(dest...))
			assert.Equal(t, 101, publisherID)
			for i := range values {
				assert.Equal(t, float64((i+1)*10), values[i])
			}
			assert.False(t, rows.Next())
			require.NoError(t, rows.Err())
		})
	}
}
