package gateway

import (
	async2 "github.com/viant/xdatly/handler/async"
	"net/http"
)

func (r *Router) NewStatusRoute() *Route {
	return &Route{
		RouteMeta: RouteMeta{
			Method: http.MethodGet,
			URL:    r.config.Meta.StatusURI,
		},
		Handler: func(writer http.ResponseWriter, req *http.Request, _ *async2.Job) {
			r.statusHandler.ServeHTTP(writer, req)
		},
	}
}
