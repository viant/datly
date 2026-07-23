package translator

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestViewBuildCacheWarmup_RootViewUsesExplicitIndexColumn(t *testing.T) {
	subject := &View{}
	viewlet := &Viewlet{Name: "adOrderRoot"}

	warmup, err := subject.buildCacheWarmup(map[string]interface{}{
		"IndexColumn":    "ad_order_id",
		"IndexParameter": "AdOrderId",
		"Limit":          "0",
		"Connector":      "bq_metrics_prewarm",
	}, viewlet)

	require.NoError(t, err)
	require.NotNil(t, warmup)
	require.Equal(t, "ad_order_id", warmup.IndexColumn)
	require.Equal(t, "AdOrderId", warmup.IndexParameter)
	require.NotNil(t, warmup.Limit)
	require.Equal(t, 0, *warmup.Limit)
	require.NotNil(t, warmup.Connector)
	require.Equal(t, "bq_metrics_prewarm", warmup.Connector.Ref)
	require.Len(t, warmup.Cases, 1)
	require.Len(t, warmup.Cases[0].Set, 0)
}

func TestViewBuildCacheWarmup_InvalidLimitReturnsError(t *testing.T) {
	subject := &View{}
	viewlet := &Viewlet{Name: "adOrderRoot"}

	warmup, err := subject.buildCacheWarmup(map[string]interface{}{
		"IndexColumn": "ad_order_id",
		"Limit":       "invalid",
	}, viewlet)

	require.Nil(t, warmup)
	require.Error(t, err)
}

func TestViewBuildCacheWarmup_NegativeLimitReturnsError(t *testing.T) {
	subject := &View{}
	viewlet := &Viewlet{Name: "adOrderRoot"}

	warmup, err := subject.buildCacheWarmup(map[string]interface{}{
		"IndexColumn": "ad_order_id",
		"Limit":       "-1",
	}, viewlet)

	require.Nil(t, warmup)
	require.Error(t, err)
}
