package handler

import (
	"github.com/viant/datly/router"
	"github.com/viant/datly/router/openapi3"
	"gopkg.in/yaml.v3"
	"net/http"
	"strconv"
)

type (
	OpenAPI struct {
		routesFn RoutesFn
		info     openapi3.Info
	}

	RoutesFn func(route string) []*router.Route
)

func (o *OpenAPI) ServeHTTP(writer http.ResponseWriter, req *http.Request) {
	query := req.URL.Query()
	routeURL := query.Get("route")

	routes := o.routesFn(routeURL)
	if len(routes) == 0 {
		writer.WriteHeader(http.StatusNotFound)
		return
	}

	spec, err := router.GenerateOpenAPI3Spec(o.info, routes...)
	if err != nil {
		writer.Write([]byte(err.Error()))
		writer.WriteHeader(http.StatusInternalServerError)
		return
	}

	specMarshal, err := yaml.Marshal(spec)
	if err != nil {
		writer.Write([]byte(err.Error()))
		writer.WriteHeader(http.StatusInternalServerError)
		return
	}

	writer.Header().Set("Content-Type", "application/x-yaml")
	writer.Header().Set("Content-Length", strconv.Itoa(len(specMarshal)))
	writer.WriteHeader(http.StatusOK)
	writer.Write(specMarshal)
}

func NewOpenApi(info openapi3.Info, routesFn RoutesFn) *OpenAPI {
	return &OpenAPI{
		routesFn: routesFn,
		info:     info,
	}
}
