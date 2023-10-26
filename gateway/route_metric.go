package gateway

import (
	"context"
	"github.com/viant/datly/repository/contract"
	"github.com/viant/gmetric"
	"net/http"
	"strings"
)

func (r *Router) NewMetricRoute() *Route {
	return &Route{
		Path: contract.NewPath(http.MethodGet, r.config.Meta.MetricURI),
		Handler: func(ctx context.Context, response http.ResponseWriter, req *http.Request) {
			r.handleMetrics(response, req)
		},
	}
}

func (r *Router) handleMetrics(writer http.ResponseWriter, req *http.Request) {
	URI := strings.Trim(r.config.Meta.MetricURI, "*")
	gmetric.NewHandler(URI, r.metrics).ServeHTTP(writer, req)
}
