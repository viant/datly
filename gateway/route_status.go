package gateway

import (
	"context"
	"github.com/viant/datly/repository/contract"
	"net/http"
)

func (r *Router) NewStatusRoute() *Route {
	return &Route{
		Path: contract.NewPath(http.MethodGet, r.config.Meta.StatusURI),
		Handler: func(ctx context.Context, writer http.ResponseWriter, req *http.Request) {
			r.statusHandler.ServeHTTP(writer, req)
		},
	}
}
