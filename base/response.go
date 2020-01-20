package base

import (
	"net/http"
)

//Response represents base response
type Response struct {
	StatusInfo
	Registry
	Headers http.Header `json:",omitempty"`
	TimeTakenMs int
}


//WriteHeaders writes headers
func (r Response) WriteHeaders(writer http.ResponseWriter)  {
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
		StatusInfo: StatusInfo{Status: StatusOK},
	}
}
