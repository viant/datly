package gateway

import "net/http"

func (r *Router) NewStatusRoute() *Route {
	return &Route{
		RouteMeta: RouteMeta{
			Method: http.MethodGet,
			URL:    r.config.Meta.StatusURI,
		},
		handler: func(writer http.ResponseWriter, req *http.Request) {
			r.statusHandler.ServeHTTP(writer, req)
		},
	}
}
