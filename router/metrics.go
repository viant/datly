package router

import "github.com/viant/datly/reader"

type Metrics struct {
	URI     string
	Metrics []*reader.Metric
}

func NewMetrics(URI string, metrics []*reader.Metric) *Metrics {
	return &Metrics{URI: URI, Metrics: metrics}
}
