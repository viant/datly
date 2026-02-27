package gateway

import (
	"context"
	"encoding/json"
	"net/http"
	"strings"
	"time"

	"github.com/viant/afs/url"
	"github.com/viant/datly/gateway/router"
	"github.com/viant/datly/repository"
	"github.com/viant/datly/repository/contract"
	"github.com/viant/datly/repository/logging"
	"github.com/viant/datly/repository/path"
	vcontext "github.com/viant/datly/view/context"
	"github.com/viant/xdatly/handler/exec"

	dlogger "github.com/viant/datly/logger"
)

const (
	RouteUnspecifiedKind = iota
	RouteWarmupKind
	RouteOpenAPIKind
)

type (
	Route struct {
		Path          *contract.Path
		MCP           *contract.ModelContextProtocol
		Meta          *contract.Meta
		Kind          int
		ApiKeys       []*path.APIKey
		Providers     []*repository.Provider
		NewMultiRoute func(routes []*contract.Path) *Route                                       `json:"-"`
		Handler       func(ctx context.Context, response http.ResponseWriter, req *http.Request) `json:"-"`
		logging.Config
		Version string

		// Counter is an optional per-route metrics counter
		Counter dlogger.Counter `json:"-"`
	}
)

func (r *Route) Handle(res http.ResponseWriter, req *http.Request) int {
	if !r.CanHandle(req) {
		write(res, http.StatusForbidden, nil)
	}
	ctx := context.Background()
	execContext := exec.NewContext(req.Method, req.RequestURI, req.Header, r.Version)
	ctx = vcontext.WithValue(ctx, exec.ContextKey, execContext)
	var onDone func(time.Time, ...interface{}) int64 = nil
	var start time.Time
	if r.Counter != nil {
		start = time.Now()
		onDone = r.Counter.Begin(start)
	}
	r.Handler(ctx, res, req)

	// finalize metrics
	if onDone != nil {
		end := time.Now()
		onDone(end)
		// Determine final status code
		statusCode := execContext.StatusCode
		if statusCode == 0 {
			statusCode = http.StatusOK
		}
		// Increment error/success buckets
		if statusCode >= 200 && statusCode < 300 {
			r.Counter.IncrementValue("Success")
			r.Counter.IncrementValue("status:2xx")
		} else if statusCode >= 400 && statusCode < 500 {
			r.Counter.IncrementValue("Error")
			r.Counter.IncrementValue("status:4xx")
		} else if statusCode >= 500 {
			r.Counter.IncrementValue("Error")
			r.Counter.IncrementValue("status:5xx")
		} else {
			// Treat other codes as success by default
			r.Counter.IncrementValue("Success")
		}
	}
	if execContext.StatusCode == 0 {
		execContext.StatusCode = http.StatusOK
	}
	logging.Log(&r.Config, execContext)
	return execContext.StatusCode
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
	route := &Route{
		Path:      &handler.Path.Path,
		MCP:       &handler.Path.ModelContextProtocol,
		Meta:      &handler.Path.Meta,
		Providers: []*repository.Provider{handler.Provider},
		Handler:   handler.HandleRequest,
		Config:    r.config.Logging,
		Version:   r.config.Version,
	}
	// Pre-register and attach per-route counter if metrics are enabled
	route.Counter = r.ensureRouteCounter(context.Background(), handler.Provider)
	return route
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
