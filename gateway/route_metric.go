package gateway

import (
	"github.com/viant/gmetric"
	async2 "github.com/viant/xdatly/handler/async"
	"net/http"
)

func (r *Router) NewMetricRoute() *Route {
	return &Route{
		RouteMeta: RouteMeta{
			Method: http.MethodGet,
			URL:    r.config.Meta.MetricURI,
		},
		Handler: func(response http.ResponseWriter, req *http.Request, _ *async2.Job) {
			r.handleMetrics(response, req)
		},
	}
}

func (r *Router) handleMetrics(writer http.ResponseWriter, req *http.Request) {
	gmetric.NewHandler(r.config.Meta.MetricURI, r.metrics).ServeHTTP(writer, req)
}
