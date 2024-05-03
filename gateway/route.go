package gateway

import (
	"context"
	"encoding/json"
	"github.com/viant/afs/url"
	"github.com/viant/datly/gateway/router"
	"github.com/viant/datly/repository"
	"github.com/viant/datly/repository/contract"
	"github.com/viant/datly/repository/path"
	"github.com/viant/xdatly/handler/exec"
	"net/http"
	"strings"
)

const (
	RouteUnspecifiedKind = iota
	RouteWarmupKind
	RouteOpenAPIKind
)

type (
	Route struct {
		Path          *contract.Path
		Kind          int
		ApiKeys       []*path.APIKey
		Providers     []*repository.Provider
		NewMultiRoute func(routes []*contract.Path) *Route
		Handler       func(ctx context.Context, response http.ResponseWriter, req *http.Request)
	}
)

func (r *Route) Handle(res http.ResponseWriter, req *http.Request) int {
	if !r.CanHandle(req) {
		write(res, http.StatusForbidden, nil)
	}
	execCtx := exec.NewContext()
	ctx := context.WithValue(context.Background(), exec.ContextKey, execCtx)
	r.Handler(ctx, res, req)
	return execCtx.StatusCode
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

func (r *Router) NewRouteHandler(handler *router.Handler) *Route {
	URI := handler.Path.URI
	if !strings.HasPrefix(URI, "/") {
		URI = "/" + URI
	}
	return &Route{
		Path:      &handler.Path.Path,
		Providers: []*repository.Provider{handler.Provider},
		Handler:   handler.HandleRequest,
	}
}

func (r *Route) URI() string {
	return r.Path.URI
}

func (r *Route) Namespaces() []string {
	namespaces := []string{"", r.Path.Method}
	return namespaces
}

func (r *Router) routeURL(newPrefix, URI string) string {
	oldPrefix := r.config.APIPrefix
	if strings.HasPrefix(URI, oldPrefix) {
		URI = strings.Trim(strings.Replace(URI, oldPrefix, "", 1), "/")
		newPrefix = strings.Trim(newPrefix, "/")
		URI = url.Join(newPrefix, URI)
	}

	if !strings.HasPrefix(URI, "/") {
		URI = "/" + URI
	}

	return URI
}

func (r *Router) metricURL(newPrefix, URI string) string {
	URI = r.routeURL(newPrefix, URI)
	if index := strings.Index(URI, "{"); index != -1 {
		fragment := URI[index:]
		if index := strings.Index(fragment, "}"); index != -1 {
			fragment = fragment[:index+1]
		}
		URI = strings.Replace(URI, fragment, "T", 1)
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
