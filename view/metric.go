package view

import (
	"github.com/viant/gmetric"
	"reflect"
)

// Metrics represents a view metrics
type Metrics struct {
	*gmetric.Service
	Method string
}

type metricsLocation struct{}

func metricLocation() string {
	return reflect.TypeOf(metricsLocation{}).PkgPath()
}
