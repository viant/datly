package gateway

import (
	"github.com/viant/datly/httputils"
	"github.com/viant/datly/router"
	"net/http"
)

func NewInterceptorRoute(aRouter *router.Router, routerInterceptor *router.RouteInterceptor) *Route {
	return &Route{
		RouteMeta: RouteMeta{
			URL: aRouter.Resource().URL,
		},
		Routes: aRouter.Routes(""),
		handler: func(response http.ResponseWriter, req *http.Request) {
			_, err := routerInterceptor.Intercept(req)
			if err != nil {
				code, message := httputils.BuildErrorResponse(err)
				write(response, code, []byte(message))
			}
		},
	}
}
