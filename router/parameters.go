package router

import (
	"net/http"
	"sync"
)

type (
	RequestParams struct {
		sync.Mutex
		OutputContentType string
		InputDataFormat   string
	}
)

func NewRequestParameters(request *http.Request, route *Route) (*RequestParams, error) {
	parameters := &RequestParams{}

	return parameters, nil
}
