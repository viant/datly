package gateway

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/viant/gmetric"
)

func TestRouteMetricProvider_ExportsRouteCounters(t *testing.T) {
	metrics := gmetric.New()
	counter := metrics.MultiOperationCounter("steward/metadata", "steward.metadata.signalPerformance.request", "signal performance request", time.Millisecond, time.Minute, 2, newRouteMetricProvider())

	counter.IncrementValue(routeRequestMetric)
	counter.IncrementValue(routeSuccessMetric)
	counter.IncrementValue(routeStatus2xxMetric)
	counter.IncrementValue(routeErrorMetric)
	counter.IncrementValue(routeStatus4xxMetric)
	counter.IncrementValue(routeStatus5xxMetric)

	operation := metrics.LookupOperation("steward.metadata.signalPerformance.request")
	require.NotNil(t, operation)

	values := map[string]int64{}
	for _, item := range operation.Counters {
		values[item.Value] = item.Count
	}

	require.Equal(t, int64(1), values[routeRequestMetric])
	require.Equal(t, int64(1), values[routeSuccessMetric])
	require.Equal(t, int64(1), values[routeStatus2xxMetric])
	require.Equal(t, int64(1), values[routeErrorMetric])
	require.Equal(t, int64(1), values[routeStatus4xxMetric])
	require.Equal(t, int64(1), values[routeStatus5xxMetric])
}
