package gateway

import (
	"github.com/viant/datly/httputils"
	"github.com/viant/datly/router"
	"github.com/viant/datly/router/async"
	"net/http"
)

func NewInterceptorRoute(aRouter *router.Router, routerInterceptor *router.RouteInterceptor) *Route {
	return &Route{
		RouteMeta: RouteMeta{
			URL: aRouter.Resource().URL,
		},
		Routes: aRouter.Routes(""),
		Handler: func(response http.ResponseWriter, req *http.Request, _ *async.Record) {
			_, err := routerInterceptor.Intercept(req)
			if err != nil {
				code, message := httputils.BuildErrorResponse(err)
				write(response, code, []byte(message))
			}
		},
	}
}
