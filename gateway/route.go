package gateway

import (
	"encoding/json"
	"github.com/viant/datly/router"
	"github.com/viant/toolbox"
	"net/http"
	"path"
	"strings"
)

const (
	RouteUnspecifiedKind = iota
	RouteWarmupKind
	RouteOpenAPIKind
)

type (
	Route struct {
		RouteMeta
		Kind          int
		ApiKeys       []*router.APIKey
		Routes        []*router.Route
		NewMultiRoute func(routes []*router.Route) *Route
		handler       func(response http.ResponseWriter, req *http.Request)
	}

	RouteMeta struct {
		Method string
		URL    string
	}
)

func (r *Route) Handle(res http.ResponseWriter, req *http.Request) {
	toolbox.Dump(r.ApiKeys)

	if !r.CanHandle(req) {
		write(res, http.StatusForbidden, nil)
		return
	}

	r.handler(res, req)
}

func (r *Route) CanHandle(req *http.Request) bool {
	for _, key := range r.ApiKeys {
		actualValue := req.Header.Get(key.Header)
		if key.Value != actualValue {
			return false
		}
	}

	return true
}

func (r *Router) NewRouteHandler(router *router.Router, route *router.Route) *Route {
	URI := route.URI
	if !strings.HasPrefix(URI, "/") {
		URI = "/" + URI
	}

	return &Route{
		RouteMeta: RouteMeta{
			Method: route.Method,
			URL:    URI,
		},
		handler: func(r http.ResponseWriter, req *http.Request) {
			err := router.HandleRoute(r, req, route)
			if err != nil {
				r.WriteHeader(http.StatusNotFound)
			}
		},
	}
}

func (r *Route) URI() string {
	return r.URL
}

func (r *Route) Namespaces() []string {
	namespaces := []string{"", r.Method}
	if r.Method != http.MethodGet {
		namespaces = append(namespaces, http.MethodOptions)
	}

	return namespaces
}

func (r *Router) routeURL(oldPrefix string, newPrefix, URI string) string {
	if strings.HasPrefix(URI, oldPrefix) {
		URI = strings.Trim(strings.Replace(URI, oldPrefix, "", 1), "/")
		newPrefix = strings.Trim(newPrefix, "/")
		URI = path.Join(newPrefix, URI)
	}

	if !strings.HasPrefix(URI, "/") {
		URI = "/" + URI
	}

	return URI
}

func write(r http.ResponseWriter, responseCode int, body []byte) {
	r.WriteHeader(responseCode)
	_, _ = r.Write(body)
}

type (
	StatusCodeError interface {
		StatusCode() int
		Error() string
		Message() string
	}

	HttpError struct {
		Code int
		Err  error
	}
)

func (h *HttpError) StatusCode() int {
	return h.Code
}

func (h *HttpError) Error() string {
	return h.Err.Error()
}

func (h *HttpError) Message() string {
	routesError, ok := h.Err.(*AvailableRoutesError)
	if !ok {
		return h.Error()
	}

	marshal, _ := json.Marshal(routesError)
	return string(marshal)
}
