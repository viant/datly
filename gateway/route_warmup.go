package gateway

import (
	"context"
	"encoding/json"
	"github.com/viant/datly/gateway/router"
	"github.com/viant/datly/gateway/warmup"
	"github.com/viant/datly/view"
	"net/http"
)

func (r *Router) NewWarmupRoute(routeMeta RouteMeta, routes ...*router.Route) *Route {
	return &Route{
		RouteMeta: routeMeta,
		Routes:    routes,
		Handler: func(ctx context.Context, response http.ResponseWriter, req *http.Request) {
			r.handleCacheWarmup(response, routes)
		},
		Kind: RouteWarmupKind,
		NewMultiRoute: func(routes []*router.Route) *Route {
			return r.NewWarmupRoute(RouteMeta{}, routes...)
		},
	}
}

func (r *Router) handleCacheWarmup(writer http.ResponseWriter, routes []*router.Route) {
	statusCode, content := r.handleCacheWarmupWithErr(routes)
	write(writer, statusCode, content)
}

func (r *Router) handleCacheWarmupWithErr(routes []*router.Route) (int, []byte) {
	var views []*view.View
	URIs := make([]string, len(routes))
	for i, route := range routes {
		views = append(views, router.ExtractCacheableViews(route)...)
		URIs[i] = route.URI
	}

	response := warmup.PreCache(func(_, _ string) ([]*view.View, error) {
		return views, nil
	}, URIs...)

	data, err := json.Marshal(response)

	if err != nil {
		return http.StatusInternalServerError, []byte(err.Error())
	}

	return http.StatusOK, data
}
