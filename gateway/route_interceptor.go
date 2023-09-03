package gateway

import (
	"context"
	"github.com/viant/datly/gateway/router"
	"github.com/viant/datly/utils/httputils"
	"net/http"
)

func NewInterceptorRoute(aRouter *router.Router, routerInterceptor *router.RouteInterceptor) *Route {
	return &Route{
		RouteMeta: RouteMeta{
			URL: aRouter.Resource().URL,
		},
		Routes: aRouter.Routes(""),
		Handler: func(ctx context.Context, response http.ResponseWriter, req *http.Request) {
			_, err := routerInterceptor.Intercept(req)
			if err != nil {
				code, message := httputils.BuildErrorResponse(err)
				write(response, code, []byte(message))
			}
		},
	}
}
