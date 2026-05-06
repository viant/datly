package inference_test

import (
	"reflect"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/viant/datly/internal/inference"
	tparser "github.com/viant/datly/internal/translator/parser"
	"github.com/viant/datly/view/state"
	"github.com/viant/sqlparser"
)

type predicateResolveRow struct {
	ID int    `sqlx:"ID"`
	CH string `sqlx:"CH"`
}

type predicateSiteRow struct {
	ID int `sqlx:"ID"`
}

func TestExtractRelationColumnPairs_WithPredicateBuilderAdjacentToJoin(t *testing.T) {
	testCases := []struct {
		name      string
		sql       string
		wantPairs [][2]string
	}{
		{
			name: "passes with seeded where clause",
			sql: `SELECT
  resolve.ID
FROM (
  SELECT site_id AS ID, 'CTV' AS CH
  FROM ctv_pubnames_sites
) resolve
JOIN (
  SELECT ID
  FROM CI_SITE
) site ON site.ID = resolve.ID
WHERE 1=1
${predicate.Builder().
   WithGroup(0).
   WithEqual(0,'resolve','CH',$Channel).
   Build("AND")}`,
			wantPairs: [][2]string{{"ID", "ID"}},
		},
		{
			name: "passes when where builder is placed directly after join",
			sql: `SELECT
  resolve.ID
FROM (
  SELECT site_id AS ID, 'CTV' AS CH
  FROM ctv_pubnames_sites
) resolve
JOIN (
  SELECT ID
  FROM CI_SITE
) site ON site.ID = resolve.ID
${predicate.Builder().
   WithGroup(0).
   WithEqual(0,'resolve','CH',$Channel).
   Build("WHERE")}`,
			wantPairs: [][2]string{{"ID", "ID"}},
		},
	}

	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			parseSQL := tparser.NormalizeSQLForParse(testCase.sql, nil)
			query, err := sqlparser.ParseQuery(parseSQL, tparser.OnVeltyExpression())
			require.NoError(t, err)
			require.Len(t, query.Joins, 1)

			join := query.Joins[0]
			pairs := inference.ExtractRelationColumnPairs(join)
			require.Equal(t, testCase.wantPairs, pairs)

			resolveSpec := newPredicateJoinSpec(t, "resolve", reflect.TypeOf(predicateResolveRow{}))
			siteSpec := newPredicateJoinSpec(t, "site", reflect.TypeOf(predicateSiteRow{}))

			err = resolveSpec.AddRelation("site", join, siteSpec, state.Many)
			require.NoError(t, err)
			require.Len(t, resolveSpec.Relations, 1)
		})
	}
}

func newPredicateJoinSpec(t *testing.T, name string, rType reflect.Type) *inference.Spec {
	t.Helper()

	aType, err := inference.NewType("test", name, rType)
	require.NoError(t, err)

	return &inference.Spec{
		Table: name,
		Type:  aType,
	}
}
