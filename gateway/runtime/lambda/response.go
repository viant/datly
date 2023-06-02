package lambda

import "net/http"

type Response struct {
	Headers    http.Header
	Buffer     []byte
	StatusCode int
}

func NewResponse() *Response {
	return &Response{
		Headers:    map[string][]string{},
		Buffer:     nil,
		StatusCode: 0,
	}
}

func (r Response) Header() http.Header {
	return r.Headers
}

func (r Response) Write(bytes []byte) (int, error) {
	r.Buffer = append(r.Buffer, bytes...)
	return len(bytes), nil
}

func (r Response) WriteHeader(statusCode int) {
	r.StatusCode = statusCode
}
