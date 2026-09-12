package view

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/viant/gmetric"
)

type namedMetric string

func TestViewMetricProvider_ExportsViewCounters(t *testing.T) {
	metrics := gmetric.New()
	counter := metrics.MultiOperationCounter("steward/metadata", "steward.metadata.softIneligibilities", "softIneligibilities performance", time.Millisecond, time.Minute, 2, newViewMetricProvider())

	counter.IncrementValue(namedMetric(successMetric))
	counter.IncrementValue(namedMetric(errorMetric))
	counter.IncrementValue(namedMetric(pendingMetric))
	counter.IncrementValue(cacheHitMetric)
	counter.IncrementValue(cacheWarmupHitMetric)
	counter.IncrementValue(cacheLazyHitMetric)
	counter.IncrementValue(cacheMissMetric)
	counter.IncrementValue(cacheMissWriteMetric)
	counter.IncrementValue(cacheErrorMetric)

	operation := metrics.LookupOperation("steward.metadata.softIneligibilities")
	require.NotNil(t, operation)

	values := map[string]int64{}
	for _, item := range operation.Counters {
		values[item.Value] = item.Count
	}

	require.Equal(t, int64(1), values[successMetric])
	require.Equal(t, int64(1), values[errorMetric])
	require.Equal(t, int64(1), values[pendingMetric])
	require.Equal(t, int64(1), values[cacheHitMetric])
	require.Equal(t, int64(1), values[cacheWarmupHitMetric])
	require.Equal(t, int64(1), values[cacheLazyHitMetric])
	require.Equal(t, int64(1), values[cacheMissMetric])
	require.Equal(t, int64(1), values[cacheMissWriteMetric])
	require.Equal(t, int64(1), values[cacheErrorMetric])
}
