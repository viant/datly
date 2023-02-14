package gateway

import (
	"encoding/json"
	"github.com/viant/datly/gateway/warmup"
	"github.com/viant/datly/router"
	"github.com/viant/datly/view"
	"net/http"
)

func (r *Router) NewWarmupRoute(URL string, routes ...*router.Route) *Route {
	return &Route{
		RouteMeta: RouteMeta{
			Method: http.MethodPost,
			URL:    URL,
		},
		Routes: routes,
		handler: func(response http.ResponseWriter, req *http.Request) {
			r.handleCacheWarmup(response, routes)
		},
		Kind: RouteWarmupKind,
		NewMultiRoute: func(routes []*router.Route) *Route {
			return r.NewWarmupRoute("", routes...)
		},
	}
}

func (r *Router) handleCacheWarmup(writer http.ResponseWriter, routes []*router.Route) {
	statusCode, content := r.handleCacheWarmupWithErr(routes)
	write(writer, statusCode, content)
}

func (r *Router) handleCacheWarmupWithErr(routes []*router.Route) (int, []byte) {
	var views []*view.View
	for _, route := range routes {
		views = append(views, router.ExtractCacheableViews(route)...)
	}

	response := warmup.PreCache(func(_, _ string) ([]*view.View, error) {
		return views, nil
	})

	data, err := json.Marshal(response)

	if err != nil {
		return http.StatusInternalServerError, []byte(err.Error())
	}

	return http.StatusOK, data
}
