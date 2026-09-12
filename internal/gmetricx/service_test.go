package gmetricx

import (
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/viant/datly/logger"
	"github.com/viant/gmetric"
	gprovider "github.com/viant/gmetric/provider"
)

func TestLookupOperationReturnsOperationSnapshot(t *testing.T) {
	metrics := gmetric.New()
	counterName := "steward.metadata.signalPerformance"
	resolver := logger.NewCounter(NewCounter(metrics, counterName, func() *gmetric.Operation {
		return metrics.MultiOperationCounter("steward/metadata", counterName, "signal performance", time.Millisecond, time.Minute, 2, gprovider.NewBasic())
	}))
	resolver.IncrementValue("pending")

	operation := LookupOperation(metrics, counterName)
	require.NotNil(t, operation)
	require.Equal(t, counterName, operation.Name)
	require.Equal(t, int64(1), operation.Counters[1].Count)
}

func TestNewCounterSurvivesOperationSliceGrowth(t *testing.T) {
	metrics := gmetric.New()
	counterName := "steward.metadata.signalPerformance"
	resolver := logger.NewCounter(NewCounter(metrics, counterName, func() *gmetric.Operation {
		return metrics.MultiOperationCounter("steward/metadata", counterName, "signal performance", time.Millisecond, time.Minute, 2, gprovider.NewBasic())
	}))

	resolver.IncrementValue("pending")

	for i := 0; i < 32; i++ {
		metrics.MultiOperationCounter("steward/metadata", fmt.Sprintf("%s.extra.%d", counterName, i), "extra", time.Millisecond, time.Minute, 2, gprovider.NewBasic())
	}

	resolver.IncrementValue("pending")

	require.Equal(t, int64(2), metrics.LookupOperationCumulativeMetric(counterName, "pending"))
}

func TestNewCounterBeginSurvivesOperationSliceGrowth(t *testing.T) {
	metrics := gmetric.New()
	counterName := "steward.metadata.signalPerformance"
	resolver := logger.NewCounter(NewCounter(metrics, counterName, func() *gmetric.Operation {
		return metrics.MultiOperationCounter("steward/metadata", counterName, "signal performance", time.Millisecond, time.Minute, 2, gprovider.NewBasic())
	}))

	started := time.Now().Add(-10 * time.Millisecond)
	onDone := resolver.Begin(started)

	for i := 0; i < 32; i++ {
		metrics.MultiOperationCounter("steward/metadata", fmt.Sprintf("%s.begin.extra.%d", counterName, i), "extra", time.Millisecond, time.Minute, 2, gprovider.NewBasic())
	}

	onDone(time.Now(), "pending")

	require.Equal(t, int64(1), metrics.LookupOperationCumulativeMetric(counterName, "count"))
	require.Equal(t, int64(1), metrics.LookupOperationCumulativeMetric(counterName, "pending"))
	require.GreaterOrEqual(t, metrics.LookupOperationCumulativeMetric(counterName, "timeTaken"), int64(1))
}
