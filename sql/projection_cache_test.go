package sql

import (
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/viant/sqlx/io/read/cache"
)

func TestCacheProjectionUsesRenderedGrouping(t *testing.T) {
	stored, err := (CacheProjection{SQL: "SELECT SUM(v) AS total,region,COUNT(*) AS count FROM sales GROUP BY region"}).Fields()
	require.NoError(t, err)
	for _, test := range []struct {
		sql string
		hit bool
	}{
		{"SELECT region,COUNT(*) AS count FROM sales GROUP BY 1", true},
		{"SELECT SUM(v) AS total FROM sales", false},
		{"SELECT region,MAX(v) AS total FROM sales GROUP BY region", false},
	} {
		requested, err := (CacheProjection{SQL: test.sql}).Fields()
		require.NoError(t, err)
		_, hit, _, err := (cache.Projection{Stored: stored}).Indexes(requested)
		require.NoError(t, err)
		require.Equal(t, test.hit, hit, test.sql)
	}
}

func TestCacheProjectionUsesOutputNamesForPlaceholderDimensionIdentity(t *testing.T) {
	stored, err := (CacheProjection{SQL: `
SELECT
  advertiser_id,
  CAST(NULL AS FLOAT64) AS default_cpm,
  CAST(NULL AS FLOAT64) AS partner_fee,
  CAST(NULL AS FLOAT64) AS fee_rate,
  CAST(NULL AS FLOAT64) AS fee_cap,
  CAST(NULL AS STRING) AS campaign_label,
  CAST(NULL AS STRING) AS vertical_label,
  CAST(NULL AS STRING) AS provider_label
FROM advertiser_report
GROUP BY 1,2,3,4,5,6,7,8`}).Fields()
	require.NoError(t, err)
	for _, field := range stored {
		require.NotEmpty(t, field.DimensionKey, field.Name)
	}
	requested, err := (CacheProjection{SQL: `
SELECT
  CAST(NULL AS STRING) AS provider_label,
  advertiser_id,
  CAST(NULL AS FLOAT64) AS partner_fee,
  CAST(NULL AS FLOAT64) AS default_cpm,
  CAST(NULL AS FLOAT64) AS fee_rate,
  CAST(NULL AS FLOAT64) AS fee_cap,
  CAST(NULL AS STRING) AS campaign_label,
  CAST(NULL AS STRING) AS vertical_label
FROM advertiser_report
GROUP BY 1,2,3,4,5,6,7,8`}).Fields()
	require.NoError(t, err)
	indexes, hit, reason, err := (cache.Projection{Stored: stored}).Indexes(requested)
	require.NoError(t, err)
	require.True(t, hit, reason)
	require.Equal(t, []int{7, 0, 2, 1, 3, 4, 5, 6}, indexes)
}

func TestCacheProjectionAllowsGroupedMeasureSubsetWithSameDimensions(t *testing.T) {
	stored, err := (CacheProjection{SQL: `
SELECT
  advertiser_id,
  CAST(NULL AS FLOAT64) AS default_cpm,
  SUM(spend) AS spend,
  COUNT(*) AS rows
FROM advertiser_report
GROUP BY 1,2`}).Fields()
	require.NoError(t, err)
	requested, err := (CacheProjection{SQL: `
SELECT
  CAST(NULL AS FLOAT64) AS default_cpm,
  advertiser_id,
  COUNT(*) AS rows
FROM advertiser_report
GROUP BY 1,2`}).Fields()
	require.NoError(t, err)
	indexes, hit, reason, err := (cache.Projection{Stored: stored}).Indexes(requested)
	require.NoError(t, err)
	require.True(t, hit, reason)
	require.Equal(t, []int{1, 0, 3}, indexes)
}

func TestCacheProjectionRejectsMissingGroupedDimension(t *testing.T) {
	stored, err := (CacheProjection{SQL: `
SELECT
  advertiser_id,
  CAST(NULL AS FLOAT64) AS default_cpm
FROM advertiser_report
GROUP BY 1,2`}).Fields()
	require.NoError(t, err)
	requested, err := (CacheProjection{SQL: `
SELECT
  advertiser_id,
  CAST(NULL AS FLOAT64) AS other_cpm
FROM advertiser_report
GROUP BY 1,2`}).Fields()
	require.NoError(t, err)
	_, hit, reason, err := (cache.Projection{Stored: stored}).Indexes(requested)
	require.NoError(t, err)
	require.False(t, hit)
	require.Equal(t, "grouped_dimension_mismatch", reason)
}

func TestCacheProjectionRejectsChangedDimensionExpressionWithSameOutputName(t *testing.T) {
	stored, err := (CacheProjection{SQL: `
SELECT
  advertiser_id,
  CAST(NULL AS FLOAT64) AS default_cpm
FROM advertiser_report
GROUP BY 1,2`}).Fields()
	require.NoError(t, err)
	requested, err := (CacheProjection{SQL: `
SELECT
  advertiser_id,
  CAST(1 AS FLOAT64) AS default_cpm
FROM advertiser_report
GROUP BY 1,2`}).Fields()
	require.NoError(t, err)
	_, hit, reason, err := (cache.Projection{Stored: stored}).Indexes(requested)
	require.NoError(t, err)
	require.False(t, hit)
	require.Equal(t, "grouped_dimension_mismatch", reason)
}
