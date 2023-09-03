package gateway

import (
	"context"
	"net/http"
)

func (r *Router) NewStatusRoute() *Route {
	return &Route{
		RouteMeta: RouteMeta{
			Method: http.MethodGet,
			URL:    r.config.Meta.StatusURI,
		},
		Handler: func(ctx context.Context, writer http.ResponseWriter, req *http.Request) {
			r.statusHandler.ServeHTTP(writer, req)
		},
	}
}
