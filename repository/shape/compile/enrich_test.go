package compile

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/viant/datly/repository/shape"
	"github.com/viant/datly/repository/shape/plan"
)

func TestApplySourceParityEnrichment_RuleConnectorAndSQLURI(t *testing.T) {
	source := &shape.Source{
		Path: "/repo/dql/platform/timezone/timezone.dql",
		DQL:  `/* {"Connector":"ci_ads"} */ SELECT * FROM CI_TIME_ZONE t`,
	}
	result := &plan.Result{
		Views: []*plan.View{
			{Name: "timezone", Table: "timezone", SQL: "SELECT * FROM CI_TIME_ZONE t"},
		},
	}

	applySourceParityEnrichment(result, source)

	require.Equal(t, "ci_ads", result.Views[0].Connector)
	require.Equal(t, "timezone/timezone.sql", result.Views[0].SQLURI)
	require.Equal(t, "CI_TIME_ZONE", result.Views[0].Table)
}

func TestApplySourceParityEnrichment_InferTableFromSubquery(t *testing.T) {
	source := &shape.Source{
		Path: "/repo/dql/platform/advertiser/advertiser.dql",
		DQL:  `SELECT x.* FROM (SELECT a.* FROM CI_ADVERTISER a) x`,
	}
	result := &plan.Result{
		Views: []*plan.View{
			{Name: "advertiser", Table: "advertiser", SQL: `SELECT x.* FROM (SELECT a.* FROM CI_ADVERTISER a) x`},
		},
	}

	applySourceParityEnrichment(result, source)

	require.Equal(t, "CI_ADVERTISER", result.Views[0].Table)
	require.Equal(t, "advertiser/advertiser.sql", result.Views[0].SQLURI)
}

func TestApplySourceParityEnrichment_InferTableFromEmbed(t *testing.T) {
	tempDir := t.TempDir()
	dqlDir := filepath.Join(tempDir, "dql", "platform", "timezone")
	require.NoError(t, os.MkdirAll(dqlDir, 0o755))
	embedded := filepath.Join(dqlDir, "timezone.sql")
	require.NoError(t, os.WriteFile(embedded, []byte(`SELECT tz.ID FROM CI_TIME_ZONE tz`), 0o644))
	source := &shape.Source{
		Path: filepath.Join(dqlDir, "timezone.dql"),
		DQL:  `SELECT timezone.* FROM (${embed: timezone.sql}) timezone`,
	}
	result := &plan.Result{
		Views: []*plan.View{
			{Name: "timezone", Table: "timezone", SQL: `SELECT timezone.* FROM (${embed: timezone.sql}) timezone`},
		},
	}

	applySourceParityEnrichment(result, source)

	require.Equal(t, "CI_TIME_ZONE", result.Views[0].Table)
	require.Equal(t, "timezone/timezone.sql", result.Views[0].SQLURI)
}

func TestTopLevelFromExpr_IgnoresNestedFrom(t *testing.T) {
	sqlText := `SELECT a.*, EXISTS(SELECT 1 FROM CI_ENTITY_WATCHLIST w WHERE w.ENTITY_ID = a.ID) AS watching FROM (SELECT x.* FROM CI_ADVERTISER x) a`
	require.Equal(t, "(SELECT x.* FROM CI_ADVERTISER x) a", topLevelFromExpr(sqlText))
}

func TestInferConnector(t *testing.T) {
	require.Equal(t, "system", inferConnector(&plan.View{Table: "session"}, &shape.Source{Path: "/repo/dql/system/session/session.dql"}))
	require.Equal(t, "ci_ads", inferConnector(&plan.View{Table: "CI_ADVERTISER"}, &shape.Source{Path: "/repo/dql/platform/advertiser/advertiser.dql"}))
	require.Equal(t, "sitemgmt", inferConnector(&plan.View{Table: "SITE_MAP"}, &shape.Source{Path: "/repo/dql/ui/agency/detail/campaign.dql"}))
}

func TestExtractSummarySQL(t *testing.T) {
	sqlText := `SELECT b.* FROM CI_BROWSER b
JOIN (
  SELECT COUNT(1) AS CNT
  FROM ($View.browser.SQL) t
) summary ON 1=1`
	require.Contains(t, extractSummarySQL(sqlText), "COUNT(1)")
}

func TestInferTableFromSQL_PreservesTemplateQualifiedTable(t *testing.T) {
	sqlText := `SELECT SITE_ID FROM ${sitemgmt_project}.${sitemgmt_dataset}.SITE_LIST_MATCH slm`
	require.Equal(t, "${sitemgmt_project}.${sitemgmt_dataset}.SITE_LIST_MATCH", inferTableFromSQL(sqlText, nil))
}

func TestShouldInferTable_NormalizedTemplatePlaceholderTable(t *testing.T) {
	require.True(t, shouldInferTable(&plan.View{Name: "match", Table: "1.1.SITE_LIST_MATCH"}))
	require.False(t, shouldInferTable(&plan.View{Name: "match", Table: "SITE_LIST_MATCH"}))
}

func TestInferTableFromSQL_PathLikeTable(t *testing.T) {
	sqlText := `SELECT user_id FROM session/attributes WHERE user_id = 1`
	require.Equal(t, "session/attributes", inferTableFromSQL(sqlText, nil))
}

func TestApplySourceParityEnrichment_InferTableFromSiblingSQLOnPlaceholderTable(t *testing.T) {
	tempDir := t.TempDir()
	dqlDir := filepath.Join(tempDir, "dql", "platform", "sitelist")
	require.NoError(t, os.MkdirAll(dqlDir, 0o755))
	require.NoError(t, os.WriteFile(filepath.Join(dqlDir, "match.sql"), []byte(`SELECT SITE_ID FROM ${sitemgmt_project}.${sitemgmt_dataset}.SITE_LIST_MATCH slm`), 0o644))
	source := &shape.Source{
		Path: filepath.Join(dqlDir, "match.dql"),
		DQL:  `SELECT 1`,
	}
	result := &plan.Result{
		Views: []*plan.View{
			{Name: "match", Table: "1.1.SITE_LIST_MATCH"},
		},
	}

	applySourceParityEnrichment(result, source)

	require.Equal(t, "${sitemgmt_project}.${sitemgmt_dataset}.SITE_LIST_MATCH", result.Views[0].Table)
}

func TestExtractJoinSubqueryBodies(t *testing.T) {
	sqlText := `SELECT sl.* FROM SITE_LIST sl
JOIN (
 SELECT SITE_ID, SITE_LIST_ID FROM ${sitemgmt_project}.${sitemgmt_dataset}.SITE_LIST_MATCH
) match ON match.SITE_LIST_ID = sl.ID
JOIN (
 ${embed: match_rules.sql}
 ${predicate.Builder().CombineOr($predicate.FilterGroup(1, "AND")).Build("WHERE")}
) matchRules ON matchRules.SITE_LIST_ID = sl.ID`
	bodies := extractJoinSubqueryBodies(sqlText)
	require.Contains(t, bodies, "match")
	require.Contains(t, bodies["match"], "SITE_LIST_MATCH")
	require.Contains(t, bodies, "matchRules")
	require.Contains(t, bodies["matchRules"], "${embed: match_rules.sql}")
}

func TestApplySourceParityEnrichment_Metadata(t *testing.T) {
	source := &shape.Source{
		Path: "/repo/dql/platform/tvaffiliatestation/tvaffiliatestation.dql",
		DQL: `/* {"Name":"TvAffiliateStation"} */
SELECT use_connector(tvAffiliateStation, 'ci_ads'),
       allow_nulls(tvAffiliateStation),
       set_limit(tvAffiliateStation, 0)
FROM CI_TV_AFFILIATE_STATION tvAffiliateStation
JOIN (
  SELECT COUNT(1) AS CNT FROM ($View.tvAffiliateStation.SQL) t
) summary ON 1=1`,
	}
	result := &plan.Result{
		Views: []*plan.View{
			{Name: "tvAffiliateStation", Table: "CI_TV_AFFILIATE_STATION", SQL: "SELECT * FROM CI_TV_AFFILIATE_STATION tvAffiliateStation"},
		},
	}
	hints := extractViewHints(source.DQL)
	applyViewHints(result, hints)
	applySourceParityEnrichment(result, source)

	require.Len(t, result.Views, 1)
	actual := result.Views[0]
	require.NotNil(t, actual.AllowNulls)
	require.True(t, *actual.AllowNulls)
	require.NotNil(t, actual.SelectorNoLimit)
	require.True(t, *actual.SelectorNoLimit)
	require.Equal(t, "tv", actual.SelectorNamespace)
	require.Equal(t, "platform/tvaffiliatestation", actual.Module)
	require.Equal(t, "*TvAffiliateStationView", actual.SchemaType)
	require.NotEmpty(t, actual.Summary)
}
