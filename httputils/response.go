package httputils

import "net/http"

type ClosableResponse struct {
	http.ResponseWriter
	Closed bool
}

func NewClosableResponse(response http.ResponseWriter) *ClosableResponse {
	return &ClosableResponse{
		ResponseWriter: response,
	}
}

func (c *ClosableResponse) WriteHeader(statusCode int) {
	c.Closed = true
	c.ResponseWriter.WriteHeader(statusCode)
}
