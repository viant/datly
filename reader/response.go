package reader

import (
	"datly/base"
	"datly/config"
	"datly/metric"
)

//Response represents
type Response struct {
	base.Response
	RuleError string          `json:",omitempty"`
	Metrics   *metric.Metrics `json:",omitempty"`
	Rule      *config.Rule    `json:",omitempty"`
}

//NewResponse creates a response
func NewResponse() *Response {
	return &Response{
		Response: *base.NewResponse(),
		Metrics:  metric.NewMetrics(),
	}
}
