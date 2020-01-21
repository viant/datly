package reader

import (
	"datly/base"
	"datly/config"
	"datly/metric"
	"time"
)

//Response represents
type Response struct {
	base.Response
	startTime   time.Time
	RuleError   string          `json:",omitempty"`
	Metrics     *metric.Metrics `json:",omitempty"`
	Rule        *config.Rule    `json:",omitempty"`
	TimeTakenMs int
}

//OnDone computes time taken
func (r *Response) OnDone() {
	r.TimeTakenMs = int(time.Now().Sub(r.startTime) / time.Millisecond)
}

//NewResponse creates a response
func NewResponse() *Response {
	return &Response{
		startTime: time.Now(),
		Response:  *base.NewResponse(),
		Metrics:   metric.NewMetrics(),
	}
}
