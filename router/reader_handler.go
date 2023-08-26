package router

import (
	"github.com/viant/datly/reader"
	"github.com/viant/datly/view"
	"net/http"
)

type (
	ReaderSession struct {
		RequestParams *RequestParams
		Route         *Route
		Request       *http.Request
		Response      http.ResponseWriter
		State         *view.ResourceState
	}

	ReaderResponse struct {
		Response       *reader.Output
		StatusCode     int
		ResponseHeader http.Header
		ContentType    string
		Value          interface{}
	}
)
