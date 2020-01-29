package contract

import (
	"github.com/viant/datly/config"
	"github.com/viant/datly/metric"
	"github.com/viant/datly/shared"
	"net/http"
	"sync"
	"time"
)

//Response represents base response
type Response struct {
	StatusInfo
	Data        map[string]interface{}
	Rule        *config.Rule `json:",omitempty"`
	RuleCount   int          `json:",omitempty"`
	Headers     http.Header  `json:",omitempty"`
	TimeTakenMs int
	startTime   time.Time
	Metrics     *metric.Metrics `json:",omitempty"`
	mux         sync.Mutex
}

func (r *Response) Put(key string, value interface{}) {
	r.mux.Lock()
	defer r.mux.Unlock()
	r.Data[key] = value
}

//OnDone computes time taken
func (r *Response) OnDone() {
	r.TimeTakenMs = int(time.Now().Sub(r.startTime) / time.Millisecond)
}

//WriteHeaders writes headers
func (r Response) WriteHeaders(writer http.ResponseWriter) {
	if len(r.Headers) == 0 {
		return
	}
	for k, values := range r.Headers {
		for i := range values {
			writer.Header().Add(k, values[i])
		}
	}
}

//NewResponse creates a response
func NewResponse() *Response {
	return &Response{
		Data:       make(map[string]interface{}),
		Metrics:    metric.NewMetrics(),
		startTime:  time.Now(),
		StatusInfo: StatusInfo{Status: shared.StatusOK},
	}
}
