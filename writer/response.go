package writer

import (
	"github.com/viant/datly/base/contract"
	"github.com/viant/datly/config"
	"github.com/viant/datly/metric"
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
