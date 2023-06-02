package gateway

import (
	"github.com/viant/datly/router"
	"github.com/viant/datly/router/async"
	"gopkg.in/yaml.v3"
	"net/http"
)

func (r *Router) NewOpenAPIRoute(URL string, routes ...*router.Route) *Route {
	return &Route{
		RouteMeta: RouteMeta{
			Method: http.MethodGet,
			URL:    URL,
		},
		Routes: routes,
		Handler: func(response http.ResponseWriter, req *http.Request, _ *async.Record) {
			r.handleOpenAPI(response, req, routes)
		},
		Kind: RouteOpenAPIKind,
		NewMultiRoute: func(routes []*router.Route) *Route {
			return r.NewOpenAPIRoute("", routes...)
		},
	}
}

func (r *Router) handleOpenAPI(res http.ResponseWriter, request *http.Request, routes []*router.Route) {
	statusCode, content := r.generateOpenAPI(routes)
	setContentType(res, statusCode, "text/yaml")
	write(res, statusCode, content)
}

func (r *Router) generateOpenAPI(routes []*router.Route) (int, []byte) {
	spec, err := router.GenerateOpenAPI3Spec(r.OpenAPIInfo, routes...)
	if err != nil {
		return http.StatusInternalServerError, []byte(err.Error())
	}

	specMarshal, err := yaml.Marshal(spec)
	if err != nil {
		return http.StatusInternalServerError, []byte(err.Error())
	}

	return http.StatusOK, specMarshal
}
