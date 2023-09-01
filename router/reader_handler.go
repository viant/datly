package router

import (
	"github.com/viant/datly/view"
	"net/http"
)

type (
	ReaderSession struct {
		RequestParams *RequestParams
		Route         *Route
		Request       *http.Request
		Response      http.ResponseWriter
		State         *view.State
	}
)
