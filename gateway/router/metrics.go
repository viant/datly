package router

import (
	"github.com/viant/xdatly/handler/response"
)

type Metrics struct {
	URI     string
	Metrics []*response.Metric
}

func NewMetrics(URI string, metrics []*response.Metric) *Metrics {
	return &Metrics{URI: URI, Metrics: metrics}
}
