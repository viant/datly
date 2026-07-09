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
		Config:  r.config.Logging,
		Version: r.config.Version,
	}
}

func (r *Router) NewGlobalMetricRoutes(URI string) []*Route {
	if !strings.HasSuffix(URI, "/") {
		URI += "/"
	}
	handler := func(ctx context.Context, response http.ResponseWriter, req *http.Request) {
		r.handleMetrics(response, req, URI)
	}
	paths := []string{
		URI + "operations",
		URI + "operation/{name}",
		URI + "operation/{name}/cumulative/{metric}",
		URI + "operation/{name}/recent/{metric}",
		URI + "operation/{name}/recent",
		URI + "counters",
		URI + "counter/{name}",
	}
	routes := make([]*Route, 0, len(paths))
	for _, pathURI := range paths {
		routes = append(routes, &Route{
			Path:    contract.NewPath(http.MethodGet, pathURI),
			Handler: handler,
			Config:  r.config.Logging,
			Version: r.config.Version,
		})
	}
	return routes
}

func (r *Router) handleMetrics(writer http.ResponseWriter, req *http.Request, URI string) {
	gmetric.NewHandler(URI, r.metrics).ServeHTTP(writer, req)
}
