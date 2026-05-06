package parser

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/viant/datly/internal/inference"
	"github.com/viant/sqlparser"
)

func TestNormalizeSQLForParse_RewritesPredicateBuildersAcrossClauses(t *testing.T) {
	state := inference.State{}
	state.Append(inference.NewConstParameter("dataset", "ci_ads"))

	input := `SELECT
  t.ID,
  COUNT(*) AS CNT
FROM ${dataset}.CI_SITE t
${predicate.Builder().CombineOr($predicate.FilterGroup(0, "AND")).Build("WHERE")}
AND t.ACTIVE = 1
AND (t.CREATED_AT >= $criteria.AppendBinding($Unsafe.From))
GROUP BY t.ID
${predicate.Builder().CombineOr($predicate.FilterGroup(1, "HAVING")).Build("HAVING")}
AND COUNT(*) > 0`

	actual := NormalizeSQLForParse(input, &state)

	require.Contains(t, actual, "FROM ci_ads.CI_SITE t")
	require.Contains(t, actual, " WHERE 1 ")
	require.Contains(t, actual, "\nAND t.ACTIVE = 1")
	require.Contains(t, actual, " >= 1")
	require.Contains(t, actual, " HAVING 1 ")
	require.NotContains(t, actual, "${predicate.Builder()")
	require.NotContains(t, actual, "$criteria.AppendBinding")

	_, err := sqlparser.ParseQuery(actual, OnVeltyExpression())
	require.NoError(t, err)
}

func TestNormalizeSQLForParse_RewritesOrPredicateBuilder(t *testing.T) {
	input := `SELECT *
FROM CI_SITE t
WHERE 1=1
${predicate.Builder().CombineOr($predicate.FilterGroup(0, "OR")).Build("OR")}
OR t.ID = 1`

	actual := NormalizeSQLForParse(input, nil)

	require.Contains(t, actual, "WHERE 1=1")
	require.Contains(t, actual, " OR 1 ")
	require.True(t, strings.Contains(actual, "OR t.ID = 1") || strings.Contains(actual, "OR  t.ID = 1"))

	_, err := sqlparser.ParseQuery(actual, OnVeltyExpression())
	require.NoError(t, err)
}

func TestNormalizeSQLForParse_PreservesViewSelectorsInSummaryJoin(t *testing.T) {
	input := `SELECT metaOrder.*
FROM (SELECT ID FROM CI_ORDER) metaOrder
JOIN (
  SELECT
    COUNT(1) / $View.Limit AS PAGE_COUNT,
    COUNT(1) AS RECORD_COUNT
  FROM ($View.metaOrder.SQL) t
) orderSummary ON 1=1`

	actual := NormalizeSQLForParse(input, nil)

	require.Contains(t, actual, "COUNT(1) / $View.Limit")
	require.Contains(t, actual, "FROM ($View.metaOrder.SQL) t")

	aQuery, err := sqlparser.ParseQuery(actual, OnVeltyExpression())
	require.NoError(t, err)
	require.Len(t, aQuery.Joins, 1)

	joinSQL := sqlparser.Stringify(aQuery.Joins[0].With)
	require.Contains(t, joinSQL, "$View.metaOrder.SQL")
	require.Contains(t, joinSQL, "$View.Limit")
}
