package httputils

import (
	"github.com/viant/xdatly/handler/response"
	"io"
	"net/http"
)

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
	response := response.BuildErrorResponse(err)
	writer.WriteHeader(response.StatusCode())
	data, _ := io.ReadAll(response.Body())
	if len(data) > 0 {
		_, _ = writer.Write(data)
	}
}
