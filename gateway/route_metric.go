package gateway

import (
	"context"
	"github.com/viant/gmetric"
	"net/http"
)

func (r *Router) NewMetricRoute() *Route {
	return &Route{
		RouteMeta: RouteMeta{
			Method: http.MethodGet,
			URL:    r.Config.Meta.MetricURI,
		},
		Handler: func(ctx context.Context, response http.ResponseWriter, req *http.Request) {
			r.handleMetrics(response, req)
		},
	}
}

func (r *Router) handleMetrics(writer http.ResponseWriter, req *http.Request) {
	gmetric.NewHandler(r.Config.Meta.MetricURI, r.Metrics).ServeHTTP(writer, req)
}
