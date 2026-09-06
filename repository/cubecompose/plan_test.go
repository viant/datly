package cubecompose

import (
	"reflect"
	"strconv"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func testCatalog(t *testing.T) *Catalog {
	catalog, err := NewCatalog(
		Field{Name: "ad_order_id", Type: reflect.TypeOf(int(0)), Role: Dimension},
		Field{Name: "publisher_id", Type: reflect.TypeOf(int(0)), Role: Dimension},
		Field{Name: "total_spend", Type: reflect.TypeOf(float64(0)), Role: Measure},
		Field{Name: "win_rate", Type: reflect.TypeOf(float64(0)), Role: Measure},
	)
	require.NoError(t, err)
	return catalog
}

func TestCompileAndRenderTopSpendDrop(t *testing.T) {
	plan, err := Compile(`SELECT t1.ad_order_id,
 t1.total_spend AS current_spend,
 t2.total_spend AS previous_spend,
 t2.total_spend - t1.total_spend AS spend_drop
 FROM $CubeSQL1 AS t1
 JOIN $CubeSQL2 AS t2 ON t1.ad_order_id = t2.ad_order_id
 WHERE t1.total_spend < t2.total_spend
 ORDER BY spend_drop DESC, t1.ad_order_id ASC
 LIMIT 5`, testCatalog(t), 2, 100)
	require.NoError(t, err)
	assert.Equal(t, 5, plan.Limit)
	require.Len(t, plan.Fields, 2)
	assert.ElementsMatch(t, []string{"ad_order_id", "total_spend"}, plan.Fields[0])
	assert.ElementsMatch(t, []string{"ad_order_id", "total_spend"}, plan.Fields[1])

	SQL, args, err := plan.Render([]Frame{
		{SQL: "SELECT ad_order_id, total_spend FROM current WHERE advertiser_id = ?", Args: []interface{}{29}},
		{SQL: "SELECT ad_order_id, total_spend FROM previous WHERE advertiser_id = ?", Args: []interface{}{29}},
	})
	require.NoError(t, err)
	assert.Contains(t, SQL, "FROM (SELECT ad_order_id, total_spend FROM current WHERE advertiser_id = ?) AS t1")
	assert.Contains(t, SQL, "JOIN (SELECT ad_order_id, total_spend FROM previous WHERE advertiser_id = ?) AS t2")
	assert.Contains(t, SQL, "ORDER BY spend_drop DESC, t1.ad_order_id ASC")
	assert.True(t, strings.HasSuffix(SQL, "LIMIT 5"))
	assert.Equal(t, []interface{}{29, 29}, args)
}

func TestCompileAndRenderThreeCubeComposition(t *testing.T) {
	plan, err := Compile(`SELECT t1.publisher_id,
 COALESCE(t1.total_spend, 0) AS current_spend,
 COALESCE(t2.total_spend, 0) AS previous_spend,
 COALESCE(t3.total_spend, 0) AS benchmark_spend,
 t1.total_spend - t2.total_spend AS period_change
 FROM $CubeSQL1 AS t1
 LEFT JOIN $CubeSQL2 AS t2 ON t1.publisher_id = t2.publisher_id
 LEFT JOIN $CubeSQL3 AS t3 ON t1.publisher_id = t3.publisher_id
 WHERE ABS(t1.total_spend - t3.total_spend) > 25
 ORDER BY period_change ASC
 LIMIT 5`, testCatalog(t), 3, 100)
	require.NoError(t, err)
	require.Len(t, plan.Fields, 3)
	for i := range plan.Fields {
		assert.ElementsMatch(t, []string{"publisher_id", "total_spend"}, plan.Fields[i])
	}

	SQL, args, err := plan.Render([]Frame{
		{SQL: "SELECT publisher_id, total_spend FROM current WHERE tenant_id = ?", Args: []interface{}{11}},
		{SQL: "SELECT publisher_id, total_spend FROM previous WHERE tenant_id = ?", Args: []interface{}{22}},
		{SQL: "SELECT publisher_id, total_spend FROM benchmark WHERE tenant_id = ?", Args: []interface{}{33}},
	})
	require.NoError(t, err)
	assert.Contains(t, SQL, "FROM (SELECT publisher_id, total_spend FROM current WHERE tenant_id = ?) AS t1")
	assert.Contains(t, SQL, "LEFT JOIN (SELECT publisher_id, total_spend FROM previous WHERE tenant_id = ?) AS t2")
	assert.Contains(t, SQL, "LEFT JOIN (SELECT publisher_id, total_spend FROM benchmark WHERE tenant_id = ?) AS t3")
	assert.Equal(t, []interface{}{int64(0), int64(0), int64(0), 11, 22, 33, int64(25)}, args)
}

func TestCompileAndRenderSingleCubeTransformation(t *testing.T) {
	plan, err := Compile(`SELECT t1.publisher_id,
 t1.total_spend * 2 AS doubled_spend
 FROM $CubeSQL1 AS t1
 ORDER BY doubled_spend DESC
 LIMIT 3`, testCatalog(t), 1, 100)
	require.NoError(t, err)
	require.Len(t, plan.Fields, 1)

	SQL, args, err := plan.Render([]Frame{{SQL: "SELECT publisher_id, total_spend FROM source WHERE tenant_id = ?", Args: []interface{}{29}}})
	require.NoError(t, err)
	assert.NotContains(t, SQL, "JOIN")
	assert.Equal(t, []interface{}{int64(2), 29}, args)
}

func TestCompileRejectsInvalidMultiCubeGraph(t *testing.T) {
	useCases := []string{
		`SELECT t1.publisher_id FROM $CubeSQL1 AS t1 JOIN $CubeSQL2 AS t2 ON t1.publisher_id = t2.publisher_id LIMIT 5`,
		`SELECT t1.publisher_id FROM $CubeSQL1 AS t1 JOIN $CubeSQL3 AS t2 ON t1.publisher_id = t2.publisher_id JOIN $CubeSQL2 AS t3 ON t1.publisher_id = t3.publisher_id LIMIT 5`,
		`SELECT t1.publisher_id FROM $CubeSQL1 AS t1 JOIN $CubeSQL2 AS t2 ON t1.publisher_id = t2.publisher_id JOIN $CubeSQL3 AS t3 ON t1.publisher_id = t2.publisher_id LIMIT 5`,
	}
	for _, SQL := range useCases {
		_, err := Compile(SQL, testCatalog(t), 3, 100)
		require.Error(t, err, SQL)
	}
}

func TestRenderRejectsPreparedCubeCountMismatch(t *testing.T) {
	plan, err := Compile(`SELECT t1.publisher_id FROM $CubeSQL1 AS t1 LIMIT 5`, testCatalog(t), 1, 100)
	require.NoError(t, err)
	_, _, err = plan.Render(nil)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "requires 1 prepared cubes")
}

func TestCompileSupportsMultiDigitCubeIndexes(t *testing.T) {
	var SQL strings.Builder
	SQL.WriteString("SELECT t10.total_spend AS value FROM $CubeSQL1 AS t1")
	for frame := 2; frame <= 10; frame++ {
		SQL.WriteString(" JOIN $CubeSQL")
		SQL.WriteString(strconv.Itoa(frame))
		SQL.WriteString(" AS t")
		SQL.WriteString(strconv.Itoa(frame))
		SQL.WriteString(" ON t1.publisher_id = t")
		SQL.WriteString(strconv.Itoa(frame))
		SQL.WriteString(".publisher_id")
	}
	SQL.WriteString(" LIMIT 5")

	plan, err := Compile(SQL.String(), testCatalog(t), 10, 100)
	require.NoError(t, err)
	require.Len(t, plan.Fields, 10)
	assert.ElementsMatch(t, []string{"publisher_id", "total_spend"}, plan.Fields[9])
}

func TestReplaceCubeMacros_OnlyRewritesExactSQLTokens(t *testing.T) {
	SQL := "SELECT '$CubeSQL2' AS label FROM $CubeSQL1 AS t1 -- $CubeSQL3\nLIMIT 1"
	normalized, err := replaceCubeMacros(SQL, 1)
	require.NoError(t, err)
	assert.Contains(t, normalized, "'$CubeSQL2'")
	assert.Contains(t, normalized, "-- $CubeSQL3")
	assert.Contains(t, normalized, "FROM "+cubeSource(1)+" AS t1")

	for _, malformed := range []string{"$CubeSQL", "$CubeSQL0", "$CubeSQL01", "$CubeSQL2", "$CubeSQL1suffix"} {
		_, err = replaceCubeMacros("SELECT 1 FROM "+malformed+" AS t1", 1)
		require.Error(t, err, malformed)
	}
}

func TestCompileRejectsUnsafeSourcesAndProjection(t *testing.T) {
	useCases := []string{
		`SELECT t1.ad_order_id FROM ORDERS t1 JOIN $CubeSQL2 t2 ON t1.ad_order_id = t2.ad_order_id LIMIT 5`,
		`SELECT t1.* FROM $CubeSQL1 t1 JOIN $CubeSQL2 t2 ON t1.ad_order_id = t2.ad_order_id LIMIT 5`,
		`SELECT t1.secret FROM $CubeSQL1 t1 JOIN $CubeSQL2 t2 ON t1.ad_order_id = t2.ad_order_id LIMIT 5`,
		`SELECT t1.total_spend FROM $CubeSQL1 t1 JOIN $CubeSQL2 t2 ON t1.total_spend = t2.total_spend LIMIT 5`,
		`SELECT t1.ad_order_id FROM $CubeSQL1 t1 JOIN $CubeSQL2 t2 ON t1.ad_order_id = t2.ad_order_id; DROP TABLE x`,
		`SELECT SAFE_DIVIDE(t1.total_spend, t2.total_spend) AS ratio FROM $CubeSQL1 t1 JOIN $CubeSQL2 t2 ON t1.ad_order_id = t2.ad_order_id LIMIT 5`,
	}
	for _, SQL := range useCases {
		_, err := Compile(SQL, testCatalog(t), 2, 100)
		assert.Error(t, err, SQL)
	}
}

func TestCompileDoesNotRenderSQLComments(t *testing.T) {
	plan, err := Compile(`SELECT t1.ad_order_id
 FROM $CubeSQL1 t1
 JOIN $CubeSQL2 t2 ON t1.ad_order_id = t2.ad_order_id
 -- the parser removes this comment and the planner renders only validated AST
 LIMIT 5`, testCatalog(t), 2, 100)
	require.NoError(t, err)
	SQL, _, err := plan.Render([]Frame{{SQL: "SELECT 1 AS ad_order_id"}, {SQL: "SELECT 1 AS ad_order_id"}})
	require.NoError(t, err)
	assert.NotContains(t, SQL, "--")
	assert.NotContains(t, SQL, "parser removes")
}

func TestCompileBindsWrapperLiteral(t *testing.T) {
	plan, err := Compile(`SELECT t1.ad_order_id FROM $CubeSQL1 t1 JOIN $CubeSQL2 t2 ON t1.ad_order_id = t2.ad_order_id WHERE t2.total_spend - t1.total_spend > 100 LIMIT 5`, testCatalog(t), 2, 100)
	require.NoError(t, err)
	SQL, args, err := plan.Render([]Frame{{SQL: "SELECT 1"}, {SQL: "SELECT 1"}})
	require.NoError(t, err)
	assert.Contains(t, SQL, "t2.total_spend - t1.total_spend > ?")
	assert.Equal(t, []interface{}{int64(100)}, args)
}

func TestRenderOrdersProjectionFrameAndCriteriaBindingsLexically(t *testing.T) {
	plan, err := Compile(`SELECT t1.ad_order_id,
 COALESCE(t1.total_spend, 0) AS current_spend
 FROM $CubeSQL1 AS t1
 JOIN $CubeSQL2 AS t2 ON t1.ad_order_id = t2.ad_order_id
 WHERE t2.total_spend - t1.total_spend > 100
 LIMIT 5`, testCatalog(t), 2, 100)
	require.NoError(t, err)

	SQL, args, err := plan.Render([]Frame{
		{SQL: "SELECT ad_order_id, total_spend FROM current WHERE advertiser_id = ? AND event_date >= ? AND event_date < ?", Args: []interface{}{29, "2026-09-05", "2026-09-06"}},
		{SQL: "SELECT ad_order_id, total_spend FROM previous WHERE advertiser_id = ? AND event_date >= ? AND event_date < ?", Args: []interface{}{29, "2026-09-04", "2026-09-05"}},
	})
	require.NoError(t, err)
	assert.Contains(t, SQL, "COALESCE(t1.total_spend, ?)")
	assert.Equal(t, []interface{}{
		int64(0),
		29, "2026-09-05", "2026-09-06",
		29, "2026-09-04", "2026-09-05",
		int64(100),
	}, args)
}

func TestCompileAndRenderDropsIncludingMissingCurrentMembers(t *testing.T) {
	plan, err := Compile(`SELECT t1.ad_order_id,
 COALESCE(t2.total_spend, 0) AS current_spend,
 t1.total_spend AS previous_spend,
 t1.total_spend - COALESCE(t2.total_spend, 0) AS spend_drop
 FROM $CubeSQL1 AS t1
 LEFT JOIN $CubeSQL2 AS t2 ON t1.ad_order_id = t2.ad_order_id
 WHERE COALESCE(t2.total_spend, 0) < t1.total_spend
 ORDER BY spend_drop DESC
 LIMIT 5`, testCatalog(t), 2, 100)
	require.NoError(t, err)

	SQL, args, err := plan.Render([]Frame{{SQL: "SELECT current"}, {SQL: "SELECT previous"}})
	require.NoError(t, err)
	assert.Contains(t, SQL, "LEFT JOIN (SELECT previous) AS t2")
	assert.Contains(t, SQL, "WHERE COALESCE(t2.total_spend, ?) < t1.total_spend")
	assert.Contains(t, SQL, "ORDER BY spend_drop DESC")
	assert.Equal(t, []interface{}{int64(0), int64(0), int64(0)}, args)
}

func TestCompileRejectsVendorSpecificOuterJoins(t *testing.T) {
	for _, join := range []string{"RIGHT JOIN", "FULL OUTER JOIN"} {
		_, err := Compile(`SELECT t1.ad_order_id FROM $CubeSQL1 AS t1 `+join+` $CubeSQL2 AS t2 ON t1.ad_order_id = t2.ad_order_id LIMIT 5`, testCatalog(t), 2, 100)
		require.Error(t, err, join)
	}
}

func TestCompileAndRenderMeasureRateChange(t *testing.T) {
	plan, err := Compile(`SELECT t1.publisher_id,
 t1.win_rate AS current_win_rate,
 t2.win_rate AS previous_win_rate,
 t1.win_rate - t2.win_rate AS win_rate_change
 FROM $CubeSQL1 AS t1
 JOIN $CubeSQL2 AS t2 ON t1.publisher_id = t2.publisher_id
 WHERE ABS(t1.win_rate - t2.win_rate) >= 0.05
 ORDER BY win_rate_change ASC
 LIMIT 10`, testCatalog(t), 2, 100)
	require.NoError(t, err)

	SQL, args, err := plan.Render([]Frame{{SQL: "SELECT current"}, {SQL: "SELECT previous"}})
	require.NoError(t, err)
	assert.Contains(t, SQL, "ABS(t1.win_rate - t2.win_rate) >= ?")
	assert.Equal(t, []interface{}{0.05}, args)
}

func TestCompileSupportsPortableRatioExpression(t *testing.T) {
	plan, err := Compile(`SELECT t1.publisher_id,
 t1.total_spend / NULLIF(t2.total_spend, 0) AS spend_ratio
 FROM $CubeSQL1 AS t1
 JOIN $CubeSQL2 AS t2 ON t1.publisher_id = t2.publisher_id
 ORDER BY spend_ratio DESC
 LIMIT 10`, testCatalog(t), 2, 100)
	require.NoError(t, err)

	SQL, args, err := plan.Render([]Frame{{SQL: "SELECT current"}, {SQL: "SELECT previous"}})
	require.NoError(t, err)
	assert.Contains(t, SQL, "t1.total_spend / NULLIF(t2.total_spend, ?)")
	assert.Equal(t, []interface{}{int64(0)}, args)
}

func TestCompileRejectsProjectionAliasInWhere(t *testing.T) {
	_, err := Compile(`SELECT t2.total_spend - t1.total_spend AS spend_drop
 FROM $CubeSQL1 AS t1
 JOIN $CubeSQL2 AS t2 ON t1.ad_order_id = t2.ad_order_id
 WHERE spend_drop > 10
 LIMIT 5`, testCatalog(t), 2, 100)
	require.Error(t, err)
}

func TestCompileSupportsSelfComparisonOperatorFamily(t *testing.T) {
	criteria := []string{
		"t1.total_spend = t2.total_spend",
		"t1.total_spend != t2.total_spend",
		"t1.total_spend > t2.total_spend",
		"t1.total_spend >= t2.total_spend",
		"t1.total_spend < t2.total_spend",
		"t1.total_spend <= t2.total_spend",
		"t1.total_spend IN (10, 20)",
		"t1.total_spend NOT IN (10, 20)",
		"t1.total_spend BETWEEN 10 AND 20",
		"t2.ad_order_id IS NULL",
		"t2.ad_order_id IS NOT NULL",
	}
	for _, criterion := range criteria {
		t.Run(criterion, func(t *testing.T) {
			plan, err := Compile(`SELECT t1.ad_order_id
 FROM $CubeSQL1 AS t1
 JOIN $CubeSQL2 AS t2 ON t1.ad_order_id = t2.ad_order_id
 WHERE `+criterion+`
 LIMIT 5`, testCatalog(t), 2, 100)
			require.NoError(t, err)
			_, _, err = plan.Render([]Frame{{SQL: "SELECT current"}, {SQL: "SELECT previous"}})
			require.NoError(t, err)
		})
	}
}

func TestCompileSupportsGroupHavingOrderAndLimit(t *testing.T) {
	plan, err := Compile(`SELECT t1.publisher_id,
 SUM(t1.total_spend) - SUM(t2.total_spend) AS spend_change
 FROM $CubeSQL1 AS t1
 JOIN $CubeSQL2 AS t2 ON t1.publisher_id = t2.publisher_id
 GROUP BY t1.publisher_id
 HAVING ABS(SUM(t1.total_spend) - SUM(t2.total_spend)) > 25
 ORDER BY spend_change ASC
 LIMIT 5`, testCatalog(t), 2, 100)
	require.NoError(t, err)

	SQL, args, err := plan.Render([]Frame{{SQL: "SELECT current"}, {SQL: "SELECT previous"}})
	require.NoError(t, err)
	assert.Contains(t, SQL, "GROUP BY t1.publisher_id")
	assert.Contains(t, SQL, "HAVING ABS(SUM(t1.total_spend) - SUM(t2.total_spend)) > ?")
	assert.Contains(t, SQL, "ORDER BY spend_change ASC")
	assert.True(t, strings.HasSuffix(SQL, "LIMIT 5"))
	assert.Equal(t, []interface{}{int64(25)}, args)
}

func TestCompileEnforcesBoundedLimit(t *testing.T) {
	baseSQL := `SELECT t1.ad_order_id
 FROM $CubeSQL1 AS t1
 JOIN $CubeSQL2 AS t2 ON t1.ad_order_id = t2.ad_order_id`
	plan, err := Compile(baseSQL, testCatalog(t), 2, 7)
	require.NoError(t, err)
	assert.Equal(t, 7, plan.Limit)
	SQL, _, err := plan.Render([]Frame{{SQL: "SELECT current"}, {SQL: "SELECT previous"}})
	require.NoError(t, err)
	assert.True(t, strings.HasSuffix(SQL, "LIMIT 7"))

	_, err = Compile(baseSQL+" LIMIT 8", testCatalog(t), 2, 7)
	require.Error(t, err)
}

func TestPlanRowTypeBuildsStructForProjection(t *testing.T) {
	plan, err := Compile(`SELECT t1.ad_order_id AS account_id,
 t1.total_spend - t2.total_spend AS spend_delta
 FROM $CubeSQL1 AS t1
 JOIN $CubeSQL2 AS t2 ON t1.ad_order_id = t2.ad_order_id
 LIMIT 5`, testCatalog(t), 2, 100)
	require.NoError(t, err)

	rowType, err := plan.RowType()
	require.NoError(t, err)
	require.Equal(t, reflect.Struct, rowType.Kind())
	require.Equal(t, 2, rowType.NumField())
	assert.Equal(t, "AccountId", rowType.Field(0).Name)
	assert.Equal(t, reflect.TypeOf(int(0)), rowType.Field(0).Type)
	assert.Equal(t, `account_id`, rowType.Field(0).Tag.Get("json"))
	assert.Equal(t, "SpendDelta", rowType.Field(1).Name)
	assert.Equal(t, reflect.TypeOf((*interface{})(nil)).Elem(), rowType.Field(1).Type)
	assert.Equal(t, `spend_delta`, rowType.Field(1).Tag.Get("sqlx"))
}

func TestPlanRowTypeFollowsEachSQLProjection(t *testing.T) {
	first, err := Compile(`SELECT t1.ad_order_id AS account_id
 FROM $CubeSQL1 AS t1
 JOIN $CubeSQL2 AS t2 ON t1.ad_order_id = t2.ad_order_id
 LIMIT 5`, testCatalog(t), 2, 100)
	require.NoError(t, err)
	second, err := Compile(`SELECT t1.ad_order_id AS account_id,
 t1.total_spend - t2.total_spend AS value_delta
 FROM $CubeSQL1 AS t1
 JOIN $CubeSQL2 AS t2 ON t1.ad_order_id = t2.ad_order_id
 LIMIT 5`, testCatalog(t), 2, 100)
	require.NoError(t, err)

	firstType, err := first.RowType()
	require.NoError(t, err)
	secondType, err := second.RowType()
	require.NoError(t, err)
	assert.NotEqual(t, firstType, secondType)
	assert.Equal(t, 1, firstType.NumField())
	assert.Equal(t, 2, secondType.NumField())
}
