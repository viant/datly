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

func WriteError(writer http.ResponseWriter, err error) {
	code, message := BuildErrorResponse(err)
	writer.WriteHeader(code)
	_, _ = writer.Write([]byte(message))
}
