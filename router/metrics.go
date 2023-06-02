package router

import "github.com/viant/datly/reader"

type Metrics struct {
	URI     string
	Metrics []*reader.Metric
	Stats   []*reader.Info
}

func NewMetrics(URI string, metrics []*reader.Metric, stats []*reader.Info) *Metrics {
	return &Metrics{URI: URI, Metrics: metrics, Stats: stats}
}
