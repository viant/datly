package gateway

import (
	"github.com/viant/datly/router"
	async2 "github.com/viant/xdatly/handler/async"
	"github.com/viant/xreflect"
	"net/http"
	"reflect"
)

func (r *Router) NewStructRoute(URL string, route *router.Route) *Route {
	return &Route{
		RouteMeta: RouteMeta{
			Method: http.MethodGet,
			URL:    URL,
		},
		Routes: []*router.Route{route},
		Handler: func(response http.ResponseWriter, req *http.Request, _ *async2.Job) {
			r.handleGolangStruct(response, route)
		},
	}
}

func (r *Router) handleGolangStruct(response http.ResponseWriter, route *router.Route) {
	statusCode, content := r.generateGoStruct(route)
	setContentType(response, statusCode, "text/plain")
	write(response, statusCode, content)
}

func (r *Router) generateGoStruct(route *router.Route) (int, []byte) {
	schemaType := route.View.Schema.CompType()
	for schemaType.Kind() == reflect.Ptr {
		schemaType = schemaType.Elem()
	}

	structContent := xreflect.GenerateStruct("GeneratedStruct", schemaType)
	return http.StatusOK, []byte(structContent)
}
