package gateway

import (
	"github.com/viant/gmetric"
	"net/http"
)

func (r *Router) NewMetricRoute() *Route {
	return &Route{
		RouteMeta: RouteMeta{
			Method: http.MethodGet,
			URL:    r.config.Meta.MetricURI,
		},
		handler: func(response http.ResponseWriter, req *http.Request) {
			r.handleMetrics(response, req)
		},
	}
}

func (r *Router) handleMetrics(writer http.ResponseWriter, req *http.Request) {
	gmetric.NewHandler(r.config.Meta.MetricURI, r.metrics).ServeHTTP(writer, req)
}