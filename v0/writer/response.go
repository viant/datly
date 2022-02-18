package writer

import (
	"github.com/viant/datly/v0/base/contract"
	"github.com/viant/datly/v0/config"
	"github.com/viant/datly/v0/metric"
)

//Response represents
type Response struct {
	contract.Response
	Rule    *config.Rule    `json:",omitempty"`
	Metrics *metric.Metrics `json:",omitempty"`
}

//NewResponse creates a response
func NewResponse() *Response {
	return &Response{
		Response: *contract.NewResponse(),
		Metrics:  metric.NewMetrics(),
	}
}
