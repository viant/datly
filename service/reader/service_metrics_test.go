package reader

import (
	"context"
	"reflect"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/viant/datly/internal/requesttrace"
	"github.com/viant/datly/logger"
	"github.com/viant/datly/view"
	"github.com/viant/datly/view/state"
	"github.com/viant/gmetric/counter"
	"github.com/viant/sqlx/io/read/cache"
	"github.com/viant/xunsafe"
)

type metricsTestCounter struct {
	values map[interface{}]int
}

func newMetricsTestCounter() *metricsTestCounter {
	return &metricsTestCounter{values: map[interface{}]int{}}
}

func (c *metricsTestCounter) Begin(started time.Time) counter.OnDone {
	return func(_ time.Time, values ...interface{}) int64 {
		for _, value := range values {
			c.values[value]++
		}
		return 0
	}
}

func (c *metricsTestCounter) DecrementValue(value interface{}) int64 {
	c.values[value]--
	return int64(c.values[value])
}

func (c *metricsTestCounter) IncrementValue(value interface{}) int64 {
	c.values[value]++
	return int64(c.values[value])
}

func TestRecordCacheReadMetrics(t *testing.T) {
	testCases := []struct {
		description string
		stats       *cache.Stats
		expected    []string
	}{
		{
			description: "warmup hit",
			stats:       &cache.Stats{Type: cache.TypeReadMulti, FoundWarmup: true},
			expected:    []string{"cache:hit", "cache:warmup_hit"},
		},
		{
			description: "lazy hit",
			stats:       &cache.Stats{Type: cache.TypeReadSingle, FoundLazy: true},
			expected:    []string{"cache:hit", "cache:lazy_hit"},
		},
		{
			description: "warmup probe miss",
			stats:       &cache.Stats{Type: cache.TypeReadMulti},
			expected:    []string{"cache:miss"},
		},
		{
			description: "lazy probe miss",
			stats:       &cache.Stats{Type: cache.TypeReadSingle},
			expected:    []string{"cache:miss"},
		},
		{
			description: "miss with write",
			stats:       &cache.Stats{Type: cache.TypeWrite},
			expected:    []string{"cache:miss", "cache:miss_write"},
		},
		{
			description: "miss",
			stats:       &cache.Stats{},
			expected:    []string{"cache:miss"},
		},
		{
			description: "error",
			stats:       &cache.Stats{ErrorType: "backend"},
			expected:    []string{"cache:error"},
		},
	}

	for _, testCase := range testCases {
		counter := newMetricsTestCounter()
		aView := &view.View{Counter: logger.NewCounter(counter)}

		recordCacheReadMetrics(aView, testCase.stats)

		for _, metric := range testCase.expected {
			require.Equalf(t, 1, counter.values[metric], testCase.description)
		}
		require.Lenf(t, counter.values, len(testCase.expected), testCase.description)
	}
}

type metricsTestRow struct {
	ID int
}

func TestAfterReadRecordsLifecycleStatusViaOnFinish(t *testing.T) {
	testCases := []struct {
		name     string
		err      error
		dryRun   bool
		expected interface{}
	}{
		{name: "success", expected: Success},
		{name: "error", err: context.Canceled, expected: Error},
		{name: "dry run success", dryRun: true, expected: Success},
		{name: "dry run error", dryRun: true, err: context.Canceled, expected: Error},
	}

	for _, testCase := range testCases {
		counter := newMetricsTestCounter()
		aView := &view.View{
			Name:    "signalPerformance",
			Schema:  state.NewSchema(reflect.TypeOf(&metricsTestRow{})),
			Counter: logger.NewCounter(counter),
		}
		dest := make([]*metricsTestRow, 0)
		collector := view.NewCollector(xunsafe.NewSlice(reflect.TypeOf(dest)), aView, &dest, nil, false)
		session := &Session{View: aView, DryRun: testCase.dryRun}

		onFinish := aView.Counter.Begin(time.Now())
		if testCase.dryRun {
			onFinish = nopCounterDone
		}
		(&Service{}).afterRead(context.Background(), session, collector, ptrTime(time.Now().Add(-time.Millisecond)), nil, testCase.err, onFinish)

		require.Equal(t, 1, counter.values[testCase.expected], testCase.name)
	}
}

func ptrTime(value time.Time) *time.Time {
	return &value
}

func TestReqTraceID(t *testing.T) {
	require.Equal(t, "unknown", reqTraceID(nil))
	require.Equal(t, "unknown", reqTraceID(context.Background()))

	ctx := requesttrace.Ensure(context.Background(), "trace-123")

	require.Equal(t, "trace-123", reqTraceID(ctx))
}
