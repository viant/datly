package reader

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/viant/datly/logger"
	"github.com/viant/datly/view"
	"github.com/viant/gmetric/counter"
	"github.com/viant/sqlx/io/read/cache"
)

type metricsTestCounter struct {
	values map[interface{}]int
}

func newMetricsTestCounter() *metricsTestCounter {
	return &metricsTestCounter{values: map[interface{}]int{}}
}

func (c *metricsTestCounter) Begin(started time.Time) counter.OnDone {
	return func(time.Time, ...interface{}) int64 { return 0 }
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
