package gateway

import (
	"context"
	"github.com/viant/datly/repository/component"
	"github.com/viant/gmetric"
	"net/http"
)

func (r *Router) NewMetricRoute() *Route {
	return &Route{
		Path: component.NewPath(http.MethodGet, r.config.Meta.MetricURI),
		Handler: func(ctx context.Context, response http.ResponseWriter, req *http.Request) {
			r.handleMetrics(response, req)
		},
	}
}

func (r *Router) handleMetrics(writer http.ResponseWriter, req *http.Request) {
	gmetric.NewHandler(r.config.Meta.MetricURI, r.metrics).ServeHTTP(writer, req)
}
