package translator

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestViewBuildCacheWarmup_RootViewUsesExplicitIndexColumn(t *testing.T) {
	subject := &View{}
	viewlet := &Viewlet{Name: "adOrderRoot"}

	warmup := subject.buildCacheWarmup(map[string]interface{}{
		"IndexColumn":    "ad_order_id",
		"IndexParameter": "AdOrderId",
		"Connector":      "bq_metrics_prewarm",
	}, viewlet)

	require.NotNil(t, warmup)
	require.Equal(t, "ad_order_id", warmup.IndexColumn)
	require.Equal(t, "AdOrderId", warmup.IndexParameter)
	require.NotNil(t, warmup.Connector)
	require.Equal(t, "bq_metrics_prewarm", warmup.Connector.Ref)
	require.Len(t, warmup.Cases, 1)
	require.Len(t, warmup.Cases[0].Set, 0)
}
