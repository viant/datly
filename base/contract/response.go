package contract

import (
	"net/http"
	"sync"
)

//Response represents base response
type Response struct {
	StatusInfo
	Data    Data        `json:",omitempty"`
	Headers http.Header `json:",omitempty"`
	mux     sync.Mutex
}

func (r *Response) Put(key string, value interface{}) {
	r.mux.Lock()
	defer r.mux.Unlock()
	r.Data[key] = value
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
		StatusInfo: NewStatusInfo(),
	}
}
