package pipeline

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/viant/datly/repository/shape/plan"
	"github.com/viant/sqlparser/expr"
	"github.com/viant/sqlparser/query"
)

func TestBuildRead(t *testing.T) {
	view, diags, err := BuildRead("orders_report", "SELECT o.id, i.sku FROM orders o JOIN items i ON o.id = i.order_id")
	require.NoError(t, err)
	require.NotNil(t, view)
	assert.Equal(t, "o", view.Name)
	assert.Equal(t, "orders", view.Table)
	assert.Equal(t, "many", view.Cardinality)
	require.Len(t, view.Relations, 1)
	assert.Equal(t, "i", view.Relations[0].Ref)
	assert.Empty(t, diags)
}

func TestBuildRead_SubqueryJoin_UsesParentNamespaceAsRoot(t *testing.T) {
	sqlText := `SELECT session.*
FROM (SELECT * FROM session WHERE user_id = $criteria.AppendBinding($Unsafe.Jwt.UserID)) session
JOIN (SELECT * FROM session/attributes) attribute ON attribute.user_id = session.user_id`
	view, _, err := BuildRead("system/session", sqlText)
	require.NoError(t, err)
	require.NotNil(t, view)
	assert.Equal(t, "session", view.Name)
	assert.Equal(t, "session", view.Table)
	assert.Contains(t, view.SQL, "$criteria.AppendBinding($Unsafe.Jwt.UserID)")
	require.NotEmpty(t, view.Relations)
	assert.Equal(t, "attribute", view.Relations[0].Ref)
}

func TestExtractRootSQLFromRaw_JoinRootTable(t *testing.T) {
	sqlText := "SELECT o.id, i.sku FROM orders o JOIN items i ON o.id = i.order_id"
	assert.Equal(t, "SELECT * FROM orders o", extractRootSQLFromRaw(sqlText))
}

func TestExtractRootSQLFromRaw_PreservesTemplateVariables(t *testing.T) {
	sqlText := `SELECT wrapper.* EXCEPT ID,
       vendor.*
FROM (SELECT ID FROM VENDOR WHERE  ID = $vendorID ) wrapper
JOIN (SELECT * FROM VENDOR t WHERE t.ID = $vendorID ) vendor ON vendor.ID = wrapper.ID`
	root := extractRootSQLFromRaw(sqlText)
	assert.Contains(t, root, "$vendorID")
	assert.NotContains(t, root, " ID = 1 ")
}

func TestNormalizeParserSQL(t *testing.T) {
	input := "SELECT * FROM session WHERE user_id = $criteria.AppendBinding($Unsafe.Jwt.UserID) AND x = $Jwt.UserID"
	actual := normalizeParserSQL(input)
	assert.NotContains(t, actual, "$criteria.AppendBinding")
	assert.NotContains(t, actual, "$Jwt.UserID")
	assert.Contains(t, actual, "user_id = 1")
}

func TestNormalizeParserSQL_VeltyBlockExpression(t *testing.T) {
	input := `SELECT b.* FROM CI_BROWSER b ${predicate.Builder().CombineOr($predicate.FilterGroup(0, "AND")).Build("WHERE")} AND b.ARCHIVED = 0`
	actual := normalizeParserSQL(input)
	assert.NotContains(t, actual, "${predicate.Builder()")
	assert.Contains(t, actual, "SELECT b.* FROM CI_BROWSER b  WHERE 1  AND b.ARCHIVED = 0")
}

func TestNormalizeParserSQL_TemplateSelector(t *testing.T) {
	input := `SELECT * FROM ${Unsafe.Vendor} t WHERE t.ID = ${Unsafe.VendorID}`
	actual := normalizeParserSQL(input)
	assert.Contains(t, actual, "FROM Unsafe_Vendor t")
	assert.Contains(t, actual, "t.ID = Unsafe_VendorID")
}

func TestNormalizeParserSQL_PrivateShorthand(t *testing.T) {
	input := `SELECT private(audience.FREQ_CAPPING) AS freq_capping FROM CI_AUDIENCE audience`
	actual := normalizeParserSQL(input)
	assert.NotContains(t, strings.ToLower(actual), "private(")
	assert.Contains(t, actual, "SELECT audience.FREQ_CAPPING AS freq_capping FROM CI_AUDIENCE audience")
}

func TestNeedsFallbackParse(t *testing.T) {
	assert.True(t, needsFallbackParse("SELECT * FROM t JOIN x ON t.id = x.id", &query.Select{}))
	assert.False(t, needsFallbackParse("SELECT * FROM t", &query.Select{From: query.From{X: expr.NewSelector("t")}}))
}

func TestBuildRead_FallbackWhenInitialParseFails(t *testing.T) {
	sqlText := `SELECT b.* FROM CI_BROWSER b ${predicate.Builder().CombineOr($predicate.FilterGroup(0, "AND")).Build("WHERE")} AND b.ARCHIVED = 0`
	view, diags, err := BuildRead("browser", sqlText)
	require.NoError(t, err)
	require.NotNil(t, view)
	assert.Equal(t, "b", view.Name)
	assert.Equal(t, "CI_BROWSER", view.Table)
	assert.Empty(t, diags)
}

func TestBuildRead_NoJoin_UsesFromSourceSQL(t *testing.T) {
	sqlText := `SELECT user.* EXCEPT MGR_ID, self_ref(user, 'Team', 'ID', 'MGR_ID') FROM (SELECT t.* FROM USER t) user`
	view, _, err := BuildRead("user_tree", sqlText)
	require.NoError(t, err)
	require.NotNil(t, view)
	assert.Equal(t, "SELECT t.* FROM USER t", strings.TrimSpace(view.SQL))
}

func TestBuildRead_ExceptBecomesInternalColumnConfig(t *testing.T) {
	sqlText := `SELECT user.* EXCEPT MGR_ID FROM (SELECT t.* FROM USER t) user`
	view, _, err := BuildRead("user_tree", sqlText)
	require.NoError(t, err)
	require.NotNil(t, view)
	require.NotNil(t, view.Declaration)
	require.NotNil(t, view.Declaration.ColumnsConfig)
	cfg, ok := view.Declaration.ColumnsConfig["MGR_ID"]
	require.True(t, ok)
	require.NotNil(t, cfg)
	assert.Equal(t, `internal:"true"`, cfg.Tag)
}

func TestBuildRead_ChildExceptBecomesRelationColumnConfig(t *testing.T) {
	sqlText := `SELECT wrapper.* EXCEPT ID,
       products.* EXCEPT VENDOR_ID,
       setting.* EXCEPT ID
FROM (SELECT ID FROM VENDOR WHERE ID = $VendorID) wrapper
JOIN (SELECT * FROM (SELECT (1) AS IS_ACTIVE, (3) AS CHANNEL, CAST($VendorID AS SIGNED) AS ID) t) setting ON setting.ID = wrapper.ID
JOIN (SELECT * FROM PRODUCT t) products ON products.VENDOR_ID = wrapper.ID`
	view, _, err := BuildRead("vendor_details", sqlText)
	require.NoError(t, err)
	require.NotNil(t, view)
	require.Len(t, view.Relations, 2)

	var productsCfg, settingCfg map[string]*plan.ViewColumnConfig
	for _, rel := range view.Relations {
		switch rel.Ref {
		case "products":
			productsCfg = rel.ColumnsConfig
		case "setting":
			settingCfg = rel.ColumnsConfig
		}
	}
	require.Contains(t, productsCfg, "VENDOR_ID")
	assert.Equal(t, `internal:"true"`, productsCfg["VENDOR_ID"].Tag)
	require.Contains(t, settingCfg, "ID")
	assert.Equal(t, `internal:"true"`, settingCfg["ID"].Tag)
}

func TestBuildRead_GroupByDoesNotMarkColumnsWithoutExplicitGrouping(t *testing.T) {
	sqlText := `SELECT t.REGION AS REGION, COUNT(*) AS TOTAL FROM SALES t GROUP BY REGION`
	view, _, err := BuildRead("sales_report", sqlText)
	require.NoError(t, err)
	require.NotNil(t, view)
	if view.Declaration != nil {
		assert.Empty(t, view.Declaration.ColumnsConfig)
	}
}

func TestBuildRead_GroupByMarksRootGroupedColumnsWithExplicitGrouping(t *testing.T) {
	sqlText := `SELECT t.REGION AS REGION, COUNT(*) AS TOTAL FROM SALES t GROUP BY REGION`
	view, _, err := BuildReadWithOptions("sales_report", sqlText, nil, map[string]bool{"t": true})
	require.NoError(t, err)
	require.NotNil(t, view)
	require.NotNil(t, view.Declaration)
	require.NotNil(t, view.Declaration.ColumnsConfig)
	cfg, ok := view.Declaration.ColumnsConfig["REGION"]
	require.True(t, ok)
	require.NotNil(t, cfg)
	require.NotNil(t, cfg.Groupable)
	assert.True(t, *cfg.Groupable)
	_, ok = view.Declaration.ColumnsConfig["TOTAL"]
	assert.False(t, ok)
}

func TestBuildRead_GroupByMarksRelationGroupedColumnsWithExplicitGrouping(t *testing.T) {
	sqlText := `SELECT vendor.REGION AS REGION, products.CATEGORY AS CATEGORY, COUNT(*) AS TOTAL
FROM VENDOR vendor
JOIN PRODUCT products ON products.VENDOR_ID = vendor.ID
GROUP BY vendor.REGION, products.CATEGORY`
	view, _, err := BuildReadWithOptions("vendor_products", sqlText, nil, map[string]bool{"vendor": true, "products": true})
	require.NoError(t, err)
	require.NotNil(t, view)
	require.NotNil(t, view.Declaration)
	require.Contains(t, view.Declaration.ColumnsConfig, "REGION")
	require.NotNil(t, view.Declaration.ColumnsConfig["REGION"].Groupable)
	assert.True(t, *view.Declaration.ColumnsConfig["REGION"].Groupable)
	require.Len(t, view.Relations, 1)
	require.Contains(t, view.Relations[0].ColumnsConfig, "CATEGORY")
	require.NotNil(t, view.Relations[0].ColumnsConfig["CATEGORY"].Groupable)
	assert.True(t, *view.Relations[0].ColumnsConfig["CATEGORY"].Groupable)
	_, ok := view.Relations[0].ColumnsConfig["TOTAL"]
	assert.False(t, ok)
}

func TestBuildRead_GroupByInRootSubqueryMarksGroupedColumnsWithExplicitGrouping(t *testing.T) {
	sqlText := `SELECT vendor.*
FROM (
    SELECT ACCOUNT_ID,
           USER_CREATED,
           SUM(ID) AS TOTAL_ID,
           MAX(ID) AS MAX_ID
    FROM VENDOR t
    GROUP BY 1, 2
) vendor`
	view, _, err := BuildReadWithOptions("vendors_grouping", sqlText, nil, map[string]bool{"vendor": true})
	require.NoError(t, err)
	require.NotNil(t, view)
	require.NotNil(t, view.Declaration)
	require.NotNil(t, view.Declaration.ColumnsConfig)
	require.Contains(t, view.Declaration.ColumnsConfig, "ACCOUNT_ID")
	require.NotNil(t, view.Declaration.ColumnsConfig["ACCOUNT_ID"].Groupable)
	assert.True(t, *view.Declaration.ColumnsConfig["ACCOUNT_ID"].Groupable)
	require.Contains(t, view.Declaration.ColumnsConfig, "USER_CREATED")
	require.NotNil(t, view.Declaration.ColumnsConfig["USER_CREATED"].Groupable)
	assert.True(t, *view.Declaration.ColumnsConfig["USER_CREATED"].Groupable)
	_, ok := view.Declaration.ColumnsConfig["TOTAL_ID"]
	assert.False(t, ok)
}

func TestBuildRead_GroupByWithQualifiedColumnsAndTemplatePredicateMarksPublisherID(t *testing.T) {
	sqlText := `SELECT
    p.event_date,
    p.agency_id,
    p.advertiser_id,
    p.campaign_id,
    p.ad_order_id,
    p.audience_id,
    p.deal_id,
    p.publisher_id,
    p.channel_id,
    p.country,
    p.site_type,
    SUM(p.bids) AS bids,
    SUM(p.impressions) AS impressions,
    SUM(p.clicks) AS clicks,
    SUM(p.conversions) AS conversions,
    SUM(p.total_spend) AS total_spend
FROM
     ` + "`viant-mediator.forecaster.fact_perf_daily_mv`" + ` p
WHERE p.event_date BETWEEN DATE_SUB(CURRENT_DATE(), INTERVAL $DateInterval DAY)
AND DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY)
    ${predicate.Builder().CombineOr($predicate.FilterGroup(0, "AND")).Build("AND")}
GROUP BY
    p.event_date,
    p.agency_id,
    p.advertiser_id,
    p.campaign_id,
    p.ad_order_id,
    p.audience_id,
    p.deal_id,
    p.publisher_id,
    p.channel_id,
    p.country,
    p.site_type`
	view, diags, err := BuildReadWithOptions("fact_perf_daily_mv", sqlText, nil, map[string]bool{"p": true})
	require.NoError(t, err)
	require.NotNil(t, view)
	require.Empty(t, diags)
	require.NotNil(t, view.Declaration)
	require.NotNil(t, view.Declaration.ColumnsConfig)
	cfg, ok := view.Declaration.ColumnsConfig["publisher_id"]
	require.True(t, ok)
	require.NotNil(t, cfg)
	require.NotNil(t, cfg.Groupable)
	assert.True(t, *cfg.Groupable)
	_, ok = view.Declaration.ColumnsConfig["total_spend"]
	assert.False(t, ok)
}

func TestBuildRead_TemplateTableSelector_PreservesRelations(t *testing.T) {
	sqlText := `SELECT vendor.*, products.*
FROM (SELECT * FROM ${Unsafe.Vendor} t WHERE t.ID IN ($criteria.AppendBinding($Unsafe.vendorIDs))) vendor
JOIN (SELECT * FROM ${Unsafe.Product} t) products ON products.VENDOR_ID = vendor.ID`
	view, _, err := BuildRead("const", sqlText)
	require.NoError(t, err)
	require.NotNil(t, view)
	assert.Equal(t, "vendor", view.Name)
	require.NotEmpty(t, view.Relations)
	assert.Equal(t, "products", view.Relations[0].Ref)
}

func TestBuildReadWithConsts_ResolvesUnsafeTablePlaceholders(t *testing.T) {
	sqlText := `SELECT vendor.*, products.*
FROM (SELECT * FROM ${Unsafe.Vendor} t WHERE t.ID IN ($criteria.AppendBinding($Unsafe.vendorIDs))) vendor
JOIN (SELECT * FROM ${Unsafe.Product} t) products ON products.VENDOR_ID = vendor.ID`
	view, _, err := BuildReadWithConsts("const", sqlText, map[string]string{
		"Vendor":  "VENDOR",
		"Product": "PRODUCT",
	})
	require.NoError(t, err)
	require.NotNil(t, view)
	assert.Equal(t, "VENDOR", view.Table)
	require.NotEmpty(t, view.Relations)
	assert.Contains(t, view.Relations[0].Table, "PRODUCT")
}
