package view

import (
	"reflect"

	"github.com/viant/gmetric/counter"
	"github.com/viant/gmetric/stat"
)

const (
	successMetric        = "Success"
	errorMetric          = "Error"
	pendingMetric        = "Pending"
	cacheHitMetric       = "cache:hit"
	cacheWarmupHitMetric = "cache:warmup_hit"
	cacheLazyHitMetric   = "cache:lazy_hit"
	cacheMissMetric      = "cache:miss"
	cacheMissWriteMetric = "cache:miss_write"
	cacheErrorMetric     = "cache:error"
)

type viewMetricProvider struct{}

var viewMetricKeys = []string{
	successMetric,
	errorMetric,
	pendingMetric,
	stat.ErrorKey,
	stat.Pending,
	cacheHitMetric,
	cacheWarmupHitMetric,
	cacheLazyHitMetric,
	cacheMissMetric,
	cacheMissWriteMetric,
	cacheErrorMetric,
}

func newViewMetricProvider() counter.Provider {
	return &viewMetricProvider{}
}

func (p *viewMetricProvider) Keys() []string {
	return viewMetricKeys
}

func (p *viewMetricProvider) Map(value interface{}) int {
	if value == nil {
		return -1
	}
	if _, ok := value.(error); ok {
		return 1
	}
	text, ok := metricText(value)
	if !ok {
		return -1
	}
	switch text {
	case successMetric:
		return 0
	case errorMetric:
		return 1
	case pendingMetric:
		return 2
	case stat.ErrorKey:
		return 3
	case stat.Pending:
		return 4
	case cacheHitMetric:
		return 5
	case cacheWarmupHitMetric:
		return 6
	case cacheLazyHitMetric:
		return 7
	case cacheMissMetric:
		return 8
	case cacheMissWriteMetric:
		return 9
	case cacheErrorMetric:
		return 10
	default:
		return -1
	}
}

func metricText(value interface{}) (string, bool) {
	if text, ok := value.(string); ok {
		return text, true
	}
	rv := reflect.ValueOf(value)
	if !rv.IsValid() || rv.Kind() != reflect.String {
		return "", false
	}
	return rv.String(), true
}
