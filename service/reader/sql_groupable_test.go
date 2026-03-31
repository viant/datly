package reader

import (
	"context"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/viant/datly/view"
)

func TestBuilder_appendColumns(t *testing.T) {
	testView := newGroupableTestView(t)
	builder := NewBuilder()

	useCases := []struct {
		description string
		selector    *view.Statelet
		expectNames []string
		expectNil   bool
		expectedSQL string
	}{
		{
			description: "default projection keeps view column order",
			selector:    view.NewStatelet(),
			expectNil:   true,
			expectedSQL: " t.region_id,  t.total_sales,  t.country_id",
		},
		{
			description: "selector projection keeps requested order",
			selector: func() *view.Statelet {
				selector := view.NewStatelet()
				selector.Columns = []string{"country_id", "region_id"}
				return selector
			}(),
			expectNames: []string{"country_id", "region_id"},
			expectedSQL: " country_id,  region_id",
		},
		{
			description: "grouped selector projection uses derived aliases for aggregate columns",
			selector: func() *view.Statelet {
				selector := view.NewStatelet()
				selector.Columns = []string{"account_id", "total_id", "max_id"}
				return selector
			}(),
			expectNames: []string{"account_id", "total_id", "max_id"},
			expectedSQL: " account_id,  total_id,  max_id",
		},
	}

	for _, useCase := range useCases {
		t.Run(useCase.description, func(t *testing.T) {
			sb := &strings.Builder{}
			viewUnderTest := testView
			if useCase.description == "grouped selector projection uses derived aliases for aggregate columns" {
				viewUnderTest = aggregateSelectorTestView(t)
			}
			projected, err := builder.appendColumns(sb, viewUnderTest, useCase.selector)
			require.NoError(t, err)
			require.Equal(t, useCase.expectedSQL, sb.String())
			if useCase.expectNil {
				require.Nil(t, projected)
				return
			}
			require.Equal(t, useCase.expectNames, columnNames(projected))
		})
	}
}

func TestBuilder_rewriteGroupBy(t *testing.T) {
	testView := newGroupableTestView(t)
	aggregateColumns := aggregateGroupableColumns()
	groupedMetrics := groupedMetricsColumns()
	builder := NewBuilder()

	useCases := []struct {
		description string
		sql         string
		allColumns  []*view.Column
		projected   []*view.Column
		expected    string
	}{
		{
			description: "replace existing group by with selected original positions",
			sql:         "(SELECT region_id, SUM(total_sales) AS total_sales, country_id FROM sales GROUP BY 1, 3)",
			allColumns:  testView.Columns,
			projected:   []*view.Column{testView.Columns[2], testView.Columns[1]},
			expected:    "(SELECT country_id, SUM(total_sales) AS total_sales FROM sales GROUP BY 1)",
		},
		{
			description: "remove group by when no selected projected column is groupable",
			sql:         "(SELECT region_id, SUM(total_sales) AS total_sales, country_id FROM sales GROUP BY 1, 3)",
			allColumns:  testView.Columns,
			projected:   []*view.Column{testView.Columns[1]},
			expected:    "(SELECT SUM(total_sales) AS total_sales FROM sales)",
		},
		{
			description: "add group by when absent",
			sql:         "(SELECT region_id, SUM(total_sales) AS total_sales, country_id FROM sales)",
			allColumns:  testView.Columns,
			projected:   []*view.Column{testView.Columns[0], testView.Columns[1]},
			expected:    "(SELECT region_id, SUM(total_sales) AS total_sales FROM sales GROUP BY 1)",
		},
		{
			description: "skip rewrite when no specific projection was selected",
			sql:         "(SELECT region_id, SUM(total_sales) AS total_sales, country_id FROM sales GROUP BY 1, 3)",
			allColumns:  testView.Columns,
			projected:   nil,
			expected:    "(SELECT region_id, SUM(total_sales) AS total_sales, country_id FROM sales GROUP BY 1, 3)",
		},
		{
			description: "rewrite grouped aggregates to selected groupable positions only",
			sql:         "(SELECT account_id, user_created, SUM(id) AS total_id, MAX(id) AS max_id FROM vendor GROUP BY 1, 2)",
			allColumns:  aggregateColumns,
			projected:   []*view.Column{aggregateColumns[0], aggregateColumns[2], aggregateColumns[3]},
			expected:    "(SELECT account_id, SUM(id) AS total_id, MAX(id) AS max_id FROM vendor GROUP BY 1)",
		},
		{
			description: "rewrite grouped aggregates does not group by nested aggregate expressions",
			sql:         "(SELECT p.channel_id, p.agency_id, ROUND(SUM(p.total_spend), 4) AS total_spend FROM last_n p GROUP BY 1, 2, 3 ORDER BY total_spend DESC LIMIT 200)",
			allColumns: func() []*view.Column {
				return []*view.Column{
					{Name: "channel_id", Groupable: true},
					{Name: "agency_id", Groupable: true},
					{Name: "total_spend"},
				}
			}(),
			projected: func() []*view.Column {
				columns := []*view.Column{
					{Name: "channel_id", Groupable: true},
					{Name: "agency_id", Groupable: true},
					{Name: "total_spend"},
				}
				return columns
			}(),
			expected: "(SELECT p.channel_id, p.agency_id, ROUND(SUM(p.total_spend), 4) AS total_spend FROM last_n p GROUP BY 1, 2 ORDER BY total_spend DESC)",
		},
		{
			description: "rewrite grouped metrics query prunes unselected dimensions from select list",
			sql:         "(SELECT p.event_date, p.agency_id, p.advertiser_id, p.campaign_id, p.ad_order_id, p.audience_id, p.deal_id, p.publisher_id, p.channel_id, p.country, p.site_type, SUM(p.bids) AS bids, SUM(p.impressions) AS impressions, SUM(p.clicks) AS clicks, SUM(p.conversions) AS conversions, SUM(p.total_spend) AS total_spend FROM `viant-mediator.forecaster.fact_perf_daily_mv` p WHERE p.event_date BETWEEN DATE_SUB(CURRENT_DATE(), INTERVAL ? DAY) AND DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY) AND (((p.agency_id = ?))) GROUP BY 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11 LIMIT 1000)",
			allColumns:  groupedMetrics,
			projected: []*view.Column{
				groupedMetrics[0],
				groupedMetrics[1],
				groupedMetrics[2],
				groupedMetrics[3],
				groupedMetrics[4],
				groupedMetrics[5],
				groupedMetrics[6],
			},
			expected: "(SELECT p.event_date, p.agency_id, p.advertiser_id, p.campaign_id, p.ad_order_id, p.audience_id, p.deal_id FROM `viant-mediator.forecaster.fact_perf_daily_mv` p WHERE p.event_date BETWEEN DATE_SUB(CURRENT_DATE(), INTERVAL ? DAY) AND DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY) AND (((p.agency_id = ?))) GROUP BY 1, 2, 3, 4, 5, 6, 7)",
		},
		{
			description: "rewrite grouped metrics CTE prunes unselected dimensions and preserves order",
			sql:         "WITH params AS (SELECT CAST(GREATEST(?, 1) AS INT64) AS date_interval), last_n AS (SELECT p.event_date, p.agency_id, p.advertiser_id, p.campaign_id, p.ad_order_id, p.audience_id, p.deal_id, p.publisher_id, p.channel_id, p.country, p.site_type, p.bids, p.impressions, p.clicks, p.conversions, p.total_spend FROM `viant-mediator.forecaster.fact_perf_daily_mv` p JOIN params prm ON TRUE WHERE p.event_date BETWEEN DATE_SUB(CURRENT_DATE(), INTERVAL prm.date_interval DAY) AND DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY) AND (((p.agency_id = ?)))) SELECT p.event_date, p.agency_id, p.advertiser_id, p.campaign_id, p.ad_order_id, p.audience_id, p.deal_id, p.publisher_id, p.channel_id, p.country, p.site_type, SUM(p.bids) AS bids, SUM(p.impressions) AS impressions, SUM(p.clicks) AS clicks, SUM(p.conversions) AS conversions, SUM(p.total_spend) AS total_spend FROM last_n p GROUP BY 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11 ORDER BY p.event_date",
			allColumns:  groupedMetrics,
			projected: []*view.Column{
				groupedMetrics[0],
				groupedMetrics[1],
				groupedMetrics[2],
				groupedMetrics[3],
				groupedMetrics[4],
				groupedMetrics[5],
				groupedMetrics[6],
			},
			expected: "WITH params AS (SELECT CAST(GREATEST(?, 1) AS INT64) AS date_interval), last_n AS (SELECT p.event_date, p.agency_id, p.advertiser_id, p.campaign_id, p.ad_order_id, p.audience_id, p.deal_id, p.publisher_id, p.channel_id, p.country, p.site_type, p.bids, p.impressions, p.clicks, p.conversions, p.total_spend FROM `viant-mediator.forecaster.fact_perf_daily_mv` p JOIN params prm ON TRUE WHERE p.event_date BETWEEN DATE_SUB(CURRENT_DATE(), INTERVAL prm.date_interval DAY) AND DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY) AND (((p.agency_id = ?)))) SELECT p.event_date, p.agency_id, p.advertiser_id, p.campaign_id, p.ad_order_id, p.audience_id, p.deal_id FROM last_n p GROUP BY 1, 2, 3, 4, 5, 6, 7 ORDER BY p.event_date",
		},
		{
			description: "rewrite grouped metrics CTE keeps selected non aggregate site_type in group by",
			sql:         "WITH params AS (SELECT CAST(GREATEST(?, 1) AS INT64) AS date_interval), last_n AS (SELECT p.event_date, p.agency_id, p.advertiser_id, p.campaign_id, p.ad_order_id, p.audience_id, p.deal_id, p.publisher_id, p.channel_id, p.country, p.site_type, p.bids, p.impressions, p.clicks, p.conversions, p.total_spend FROM `viant-mediator.forecaster.fact_perf_daily_mv` p JOIN params prm ON TRUE WHERE p.event_date BETWEEN DATE_SUB(CURRENT_DATE(), INTERVAL prm.date_interval DAY) AND DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY) AND (((p.agency_id = ?)) AND ((p.campaign_id IN (?))))) SELECT p.event_date, p.agency_id, p.advertiser_id, p.campaign_id, p.ad_order_id, p.audience_id, p.site_type, SUM(p.bids) AS bids, SUM(p.impressions) AS impressions, SUM(p.clicks) AS clicks, SUM(p.conversions) AS conversions, SUM(p.total_spend) AS total_spend FROM last_n p GROUP BY 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11 ORDER BY p.event_date LIMIT 1000",
			allColumns: func() []*view.Column {
				cloned := cloneColumns(groupedMetrics)
				cloned[10].Groupable = false
				return cloned
			}(),
			projected: func() []*view.Column {
				cloned := cloneColumns(groupedMetrics)
				cloned[10].Groupable = false
				return []*view.Column{
					cloned[0],
					cloned[1],
					cloned[2],
					cloned[3],
					cloned[4],
					cloned[5],
					cloned[10],
					cloned[11],
					cloned[12],
					cloned[13],
					cloned[14],
					cloned[15],
				}
			}(),
			expected: "WITH params AS (SELECT CAST(GREATEST(?, 1) AS INT64) AS date_interval), last_n AS (SELECT p.event_date, p.agency_id, p.advertiser_id, p.campaign_id, p.ad_order_id, p.audience_id, p.deal_id, p.publisher_id, p.channel_id, p.country, p.site_type, p.bids, p.impressions, p.clicks, p.conversions, p.total_spend FROM `viant-mediator.forecaster.fact_perf_daily_mv` p JOIN params prm ON TRUE WHERE p.event_date BETWEEN DATE_SUB(CURRENT_DATE(), INTERVAL prm.date_interval DAY) AND DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY) AND (((p.agency_id = ?)) AND ((p.campaign_id IN (?))))) SELECT p.event_date, p.agency_id, p.advertiser_id, p.campaign_id, p.ad_order_id, p.audience_id, p.site_type, SUM(p.bids) AS bids, SUM(p.impressions) AS impressions, SUM(p.clicks) AS clicks, SUM(p.conversions) AS conversions, SUM(p.total_spend) AS total_spend FROM last_n p GROUP BY 1, 2, 3, 4, 5, 6, 7 ORDER BY p.event_date",
		},
		{
			description: "rewrite grouped metrics with publisher subset renumbers group by after pruning",
			sql:         "(SELECT p.event_date, p.agency_id, p.advertiser_id, p.campaign_id, p.ad_order_id, p.audience_id, p.deal_id, p.publisher_id, p.channel_id, p.country, p.site_type, SUM(p.bids) AS bids, SUM(p.impressions) AS impressions, SUM(p.clicks) AS clicks, SUM(p.conversions) AS conversions, SUM(p.total_spend) AS total_spend FROM `viant-mediator.forecaster.fact_perf_daily_mv` p WHERE p.event_date BETWEEN DATE_SUB(CURRENT_DATE(), INTERVAL ? DAY) AND DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY) AND (((p.agency_id = ?))) GROUP BY 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11 LIMIT 1000)",
			allColumns:  groupedMetrics,
			projected: []*view.Column{
				groupedMetrics[0],
				groupedMetrics[1],
				groupedMetrics[2],
				groupedMetrics[3],
				groupedMetrics[4],
				groupedMetrics[5],
				groupedMetrics[7],
			},
			expected: "(SELECT p.event_date, p.agency_id, p.advertiser_id, p.campaign_id, p.ad_order_id, p.audience_id, p.publisher_id FROM `viant-mediator.forecaster.fact_perf_daily_mv` p WHERE p.event_date BETWEEN DATE_SUB(CURRENT_DATE(), INTERVAL ? DAY) AND DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY) AND (((p.agency_id = ?))) GROUP BY 1, 2, 3, 4, 5, 6, 7)",
		},
		{
			description: "rewrite grouped report projection drops order by on pruned dimension",
			sql:         "WITH last_n AS (SELECT p.event_date, p.agency_id, p.advertiser_id, p.campaign_id, p.ad_order_id, p.audience_id, p.deal_id, p.publisher_id, p.channel_id, p.country, p.site_type, p.bids, p.impressions, p.clicks, p.conversions, p.total_spend FROM `viant-mediator.forecaster.fact_perf_daily_mv` p WHERE p.event_date BETWEEN DATE(DATE_SUB(CURRENT_DATE(), INTERVAL 7 DAY)) AND DATE(CURRENT_DATE()-1)) SELECT p.ad_order_id, SUM(p.bids) AS bids FROM last_n p GROUP BY 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11 ORDER BY p.event_date LIMIT 1000",
			allColumns:  groupedMetrics,
			projected: []*view.Column{
				groupedMetrics[4],
				groupedMetrics[11],
			},
			expected: "WITH last_n AS (SELECT p.event_date, p.agency_id, p.advertiser_id, p.campaign_id, p.ad_order_id, p.audience_id, p.deal_id, p.publisher_id, p.channel_id, p.country, p.site_type, p.bids, p.impressions, p.clicks, p.conversions, p.total_spend FROM `viant-mediator.forecaster.fact_perf_daily_mv` p WHERE p.event_date BETWEEN DATE(DATE_SUB(CURRENT_DATE(), INTERVAL 7 DAY)) AND DATE(CURRENT_DATE()-1)) SELECT p.ad_order_id, SUM(p.bids) AS bids FROM last_n p GROUP BY 1",
		},
	}

	for _, useCase := range useCases {
		t.Run(useCase.description, func(t *testing.T) {
			actual, err := builder.rewriteGroupBy(useCase.sql, useCase.allColumns, useCase.projected)
			require.NoError(t, err)
			require.Equal(t, normalizeSQL(useCase.expected), normalizeSQL(actual))
		})
	}
}

func TestBuilder_appendRelationColumn_UsesProjectedRelationAliasForGroupedDerivedView(t *testing.T) {
	builder := NewBuilder()
	aView := view.NewView("disqualified", "disqualified",
		view.WithConnector(view.NewConnector("test", "sqlite3", ":memory:")),
		view.WithColumns(view.Columns{
			&view.Column{Name: "TAXONOMY_ID", DataType: "int"},
			&view.Column{Name: "IS_DISQUALIFIED", DataType: "int"},
		}),
	)
	require.NoError(t, aView.Init(context.Background(), view.EmptyResource()))

	relation := &view.Relation{
		Of: &view.ReferenceView{
			On: view.Links{
				&view.Link{Field: "TaxonomyId", Column: "dq.SEGMENT_ID"},
			},
		},
	}

	t.Run("default projection does not append raw source column when projected alias exists", func(t *testing.T) {
		sb := &strings.Builder{}
		require.NoError(t, builder.checkViewAndAppendRelColumn(sb, aView, relation))
		require.Equal(t, "", sb.String())
	})

	t.Run("selector projection appends projected alias expression instead of raw source column", func(t *testing.T) {
		sb := &strings.Builder{}
		selector := view.NewStatelet()
		selector.Columns = []string{"IS_DISQUALIFIED"}
		selector.Init(aView)
		require.NoError(t, builder.checkSelectorAndAppendRelColumn(sb, aView, selector, relation))
		require.Equal(t, ",  TAXONOMY_ID", sb.String())
	})
}

func TestBuilder_appendRelationColumn_UsesProjectedAliasForQualifiedSourceRelation(t *testing.T) {
	builder := NewBuilder()
	aView := view.NewView("comscoreContextual", "comscoreContextual",
		view.WithConnector(view.NewConnector("test", "sqlite3", ":memory:")),
		view.WithColumns(view.Columns{
			&view.Column{Name: "COMSCORE_CONTEXTUAL_VALUE", DataType: "string", Tag: `source:"t2.SEGMENT_ID"`},
			&view.Column{Name: "NAME", DataType: "string"},
		}),
	)
	require.NoError(t, aView.Init(context.Background(), view.EmptyResource()))

	relation := &view.Relation{
		Of: &view.ReferenceView{
			On: view.Links{
				&view.Link{Field: "ComscoreContextualValue", Column: "t2.SEGMENT_ID"},
			},
		},
	}

	require.NoError(t, relation.Of.On.Init("comscoreContextual", aView))

	t.Run("default projection does not append raw unqualified source column", func(t *testing.T) {
		sb := &strings.Builder{}
		require.NoError(t, builder.checkViewAndAppendRelColumn(sb, aView, relation))
		require.Equal(t, "", sb.String())
	})

	t.Run("selector projection appends projected alias instead of raw source column", func(t *testing.T) {
		sb := &strings.Builder{}
		selector := view.NewStatelet()
		selector.Columns = []string{"NAME"}
		selector.Init(aView)
		require.NoError(t, builder.checkSelectorAndAppendRelColumn(sb, aView, selector, relation))
		require.Equal(t, ",  COMSCORE_CONTEXTUAL_VALUE", sb.String())
	})
}

func newGroupableTestView(t *testing.T) *view.View {
	t.Helper()
	trueValue := true
	aView := view.NewView("sales", "sales",
		view.WithGroupable(true),
		view.WithConnector(view.NewConnector("test", "sqlite3", ":memory:")),
		view.WithColumns(view.Columns{
			&view.Column{Name: "region_id", DataType: "string"},
			&view.Column{Name: "total_sales", DataType: "float64"},
			&view.Column{Name: "country_id", DataType: "string"},
		}),
	)
	aView.ColumnsConfig = map[string]*view.ColumnConfig{
		"region_id":  {Name: "region_id", Groupable: &trueValue},
		"country_id": {Name: "country_id", Groupable: &trueValue},
	}
	require.NoError(t, aView.Init(context.Background(), view.EmptyResource()))
	return aView
}

func aggregateGroupableColumns() []*view.Column {
	return []*view.Column{
		{Name: "account_id", Groupable: true},
		{Name: "user_created", Groupable: true},
		{Name: "total_id"},
		{Name: "max_id"},
	}
}

func aggregateSelectorTestView(t *testing.T) *view.View {
	t.Helper()
	aView := view.NewView("vendor", "vendor",
		view.WithGroupable(true),
		view.WithConnector(view.NewConnector("test", "sqlite3", ":memory:")),
		view.WithColumns(view.Columns{
			&view.Column{Name: "account_id", DataType: "int", Groupable: true},
			&view.Column{Name: "user_created", DataType: "int", Groupable: true},
			&view.Column{Name: "total_id", DataType: "float64", Expression: "SUM(id)", Aggregate: true},
			&view.Column{Name: "max_id", DataType: "int", Expression: "MAX(id)", Aggregate: true},
		}),
	)
	require.NoError(t, aView.Init(context.Background(), view.EmptyResource()))
	return aView
}

func groupedMetricsColumns() []*view.Column {
	return []*view.Column{
		{Name: "event_date", Groupable: true},
		{Name: "agency_id", Groupable: true},
		{Name: "advertiser_id", Groupable: true},
		{Name: "campaign_id", Groupable: true},
		{Name: "ad_order_id", Groupable: true},
		{Name: "audience_id", Groupable: true},
		{Name: "deal_id", Groupable: true},
		{Name: "publisher_id", Groupable: true},
		{Name: "channel_id", Groupable: true},
		{Name: "country", Groupable: true},
		{Name: "site_type", Groupable: true},
		{Name: "bids"},
		{Name: "impressions"},
		{Name: "clicks"},
		{Name: "conversions"},
		{Name: "total_spend"},
	}
}

func cloneColumns(columns []*view.Column) []*view.Column {
	result := make([]*view.Column, len(columns))
	for i, column := range columns {
		if column == nil {
			continue
		}
		cloned := *column
		result[i] = &cloned
	}
	return result
}

func columnNames(columns []*view.Column) []string {
	result := make([]string, len(columns))
	for i, column := range columns {
		result[i] = column.Name
	}
	return result
}

func normalizeSQL(SQL string) string {
	return strings.Join(strings.Fields(SQL), " ")
}
