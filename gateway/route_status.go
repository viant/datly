package gateway

import (
	"github.com/viant/datly/router/async"
	"net/http"
)

func (r *Router) NewStatusRoute() *Route {
	return &Route{
		RouteMeta: RouteMeta{
			Method: http.MethodGet,
			URL:    r.config.Meta.StatusURI,
		},
		Handler: func(writer http.ResponseWriter, req *http.Request, _ *async.Record) {
			r.statusHandler.ServeHTTP(writer, req)
		},
	}
}
