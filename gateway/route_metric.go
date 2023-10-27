package gateway

import (
	"context"
	"github.com/viant/datly/repository/contract"
	"github.com/viant/gmetric"
	"net/http"
	"strings"
)

func (r *Router) NewMetricRoute(URI string) *Route {
	if !strings.HasSuffix(URI, "/") {
		URI += "/"
	}
	pathURI := URI
	if !strings.HasSuffix(pathURI, "*") {
		pathURI += "*"
	}
	return &Route{
		Path: contract.NewPath(http.MethodGet, pathURI),
		Handler: func(ctx context.Context, response http.ResponseWriter, req *http.Request) {
			r.handleMetrics(response, req, URI)
		},
	}
}

func (r *Router) handleMetrics(writer http.ResponseWriter, req *http.Request, URI string) {
	gmetric.NewHandler(URI, r.metrics).ServeHTTP(writer, req)
}
