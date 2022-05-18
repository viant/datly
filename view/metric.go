package view

import (
	"github.com/viant/gmetric"
	"reflect"
)

type Metrics struct {
	*gmetric.Service
	URIPart string
}

type metricsLocation struct {
}

func metricLocation() string {
	return reflect.TypeOf(metricsLocation{}).PkgPath()
}
