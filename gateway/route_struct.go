package gateway

import (
	"github.com/viant/datly/router"
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
		handler: func(response http.ResponseWriter, req *http.Request) {
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
	schemaType := route.View.Schema.Type()
	for schemaType.Kind() == reflect.Ptr {
		schemaType = schemaType.Elem()
	}

	structContent := xreflect.GenerateStruct("GeneratedStruct", schemaType)
	return http.StatusOK, []byte(structContent)
}
