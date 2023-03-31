package gateway

import (
	"encoding/json"
	"fmt"
	furl "github.com/viant/afs/url"
	"github.com/viant/cloudless/gateway/matcher"
	"github.com/viant/datly/gateway/runtime/meta"
	"github.com/viant/datly/gateway/warmup"
	"github.com/viant/datly/httputils"
	"github.com/viant/datly/router"
	"github.com/viant/datly/router/openapi3"
	"github.com/viant/datly/view"
	"github.com/viant/gmetric"
	"net/http"
	"net/url"
	"os"
	"strings"
)

//const wildcard = `{DATLY_WILDCARD}`

type (
	Router struct {
		routeMatcher            *matcher.Matcher
		localInterceptorMatcher *matcher.Matcher
		apiKeyMatcher           *matcher.Matcher
		config                  *Config
		OpenAPIInfo             openapi3.Info
		metrics                 *gmetric.Service
		statusHandler           http.Handler
		authorizer              Authorizer
		interceptors            []*router.RouteInterceptor
		routes                  []*RouteMeta
	}

	AvailableRoutesError struct {
		Message string
		Routes  []*RouteMeta
	}

	ApiKeyWrapper struct {
		apiKey *router.APIKey
	}
)

func (a *ApiKeyWrapper) URI() string {
	return a.apiKey.URI
}

func (a *ApiKeyWrapper) Namespaces() []string {
	return []string{""}
}

func (a *AvailableRoutesError) Error() string {
	marshal, _ := json.Marshal(a)
	return string(marshal)
}

//NewRouter creates new router
func NewRouter(routersIndex map[string]*router.Router, config *Config, metrics *gmetric.Service, statusHandler http.Handler, authorizer Authorizer, interceptors []*router.RouteInterceptor) *Router {
	r := &Router{
		config:                  config,
		metrics:                 metrics,
		statusHandler:           statusHandler,
		authorizer:              authorizer,
		apiKeyMatcher:           newApiKeyMatcher(config.APIKeys),
		localInterceptorMatcher: newLocalInterceptorMatcher(routersIndex),
		interceptors:            interceptors,
	}

	r.init(routersIndex)

	return r
}

func newApiKeyMatcher(keys router.APIKeys) *matcher.Matcher {
	matchables := make([]matcher.Matchable, 0, len(keys))
	for i := range keys {
		matchables = append(matchables,
			&ApiKeyWrapper{
				apiKey: keys[i],
			},
		)
	}

	return matcher.NewMatcher(matchables)
}

func (r *Router) Handle(writer http.ResponseWriter, request *http.Request) {
	err := r.ensureRequestURL(request)
	if err != nil {
		r.handleErrIfNeeded(writer, http.StatusInternalServerError, err)
		return
	}

	if !r.interceptIfNeeded(writer, request) {
		return
	}

	if !r.authorizeRequestIfNeeded(writer, request) {
		return
	}

	errStatusCode, err := r.handle(writer, request)
	r.handleErrIfNeeded(writer, errStatusCode, err)
}

func (r *Router) handle(writer http.ResponseWriter, request *http.Request) (int, error) {
	if !meta.IsAuthorized(request, r.config.Meta.AllowedSubnet) {
		return http.StatusForbidden, nil
	}

	aRoute, err := r.Match(request.Method, request.URL.Path, request)
	if err != nil {
		return http.StatusNotFound, err
	}

	aRoute.Handle(writer, request)
	return http.StatusOK, nil
}

func (r *Router) handleRoute(writer http.ResponseWriter, request *http.Request, aRouter *router.Router, aRoute *router.Route) (int, error) {
	if err := aRouter.HandleRoute(writer, request, aRoute); err != nil {
		return http.StatusNotFound, err
	}

	return http.StatusOK, nil
}

func (r *Router) Match(method, URL string, req *http.Request) (*Route, error) {
	matched := r.routeMatcher.MatchAll(method, URL)
	switch len(matched) {
	case 0:
		return nil, r.availableRoutesErr(http.StatusNotFound, fmt.Errorf("not found route with Method: %v and URL: %v", method, URL))
	case 1:
		asRoute, ok := matched[0].(*Route)
		if !ok {
			return nil, r.unexpectedType(asRoute, matched[0])
		}

		return asRoute, nil

	default:
		var routes []*router.Route
		var lastMatched *Route
		for _, matchable := range matched {
			asRoute, ok := matchable.(*Route)
			if !ok {
				return nil, r.unexpectedType(asRoute, matched[0])
			}

			if req != nil && !asRoute.CanHandle(req) {
				continue
			}

			if asRoute.NewMultiRoute == nil {
				return nil, r.availableRoutesErr(http.StatusNotFound, fmt.Errorf("found more than one route with Method: %v and URL: %v", method, URL))
			}

			if lastMatched == nil {
				lastMatched = asRoute
				continue
			}

			if lastMatched.Kind != asRoute.Kind {
				return nil, r.availableRoutesErr(http.StatusNotFound, fmt.Errorf("found more than one route with Method: %v and URL: %v", method, URL))
			}

			routes = append(routes, asRoute.Routes...)
		}

		if len(routes) == 0 {
			return nil, r.availableRoutesErr(http.StatusForbidden, fmt.Errorf("Forbidden"))
		}

		return lastMatched.NewMultiRoute(routes), nil
	}
}

func (r *Router) unexpectedType(asRoute *Route, expected interface{}) error {
	return fmt.Errorf("unexpected Matchable type, wanted %T got %T", asRoute, expected)
}

func (r *Router) handleErrIfNeeded(writer http.ResponseWriter, errStatusCode int, err error) {
	if errStatusCode < 400 {
		return
	}

	var message string
	if err != nil {
		switch actual := err.(type) {
		case StatusCodeError:
			errStatusCode = actual.StatusCode()
			message = actual.Message()
		default:
			message = err.Error()
		}
	}

	write(writer, errStatusCode, []byte(message))
}

func (r *Router) ensureRequestURL(request *http.Request) error {
	if request.URL != nil {
		return nil
	}

	URI := request.RequestURI
	if strings.Contains(URI, "://") {
		_, URI = furl.Base(URI, "https")
	}

	host := os.Getenv("FUNCTION_NAME")
	if host == "" {
		host = request.Host
	}

	if host == "" {
		host = "localhost"
	}

	URL := "https://" + host + "/" + URI
	var err error
	request.URL, err = url.Parse(URL)
	return err
}

func (r *Router) authorizeRequestIfNeeded(writer http.ResponseWriter, request *http.Request) bool {
	if r.authorizer == nil {
		return true
	}

	return r.authorizer.Authorize(writer, request)
}

func (r *Router) PreCacheables(method string, uri string) ([]*view.View, error) {
	route, err := r.Match(method, uri, nil)
	if err != nil {
		return nil, err
	}

	return r.extractCacheableViews(route.Routes...)(method, uri)
}

func (r *Router) availableRoutesErr(statusCode int, err error) error {
	return &HttpError{
		Code: statusCode,
		Err: &AvailableRoutesError{
			Message: err.Error(),
			Routes:  r.routes,
		},
	}
}

func (r *Router) extractCacheableViews(routes ...*router.Route) warmup.PreCachables {
	return func(_, _ string) ([]*view.View, error) {
		return router.ExtractCacheableViews(routes...), nil
	}
}

func (r *Router) init(routersIndex map[string]*router.Router) {
	routers := asRouterSlice(routersIndex)
	r.routeMatcher, r.routes = r.newMatcher(routers)
}

func asRouterSlice(routers map[string]*router.Router) []*router.Router {
	result := make([]*router.Router, len(routers))

	i := 0
	for aKey := range routers {
		result[i] = routers[aKey]
		i++
	}

	return result
}

func (r *Router) newMatcher(routers []*router.Router) (*matcher.Matcher, []*RouteMeta) {
	routesSize := 0
	for _, aRouter := range routers {
		routesSize += len(aRouter.Routes("")) * 3
	}

	routes := make([]*Route, 0, routesSize)

	for _, aRouter := range routers {
		routerRoutes := aRouter.Routes("")

		for _, route := range routerRoutes {
			routesLen := len(routes)

			var apiKeys []*router.APIKey
			if matched := r.config.APIKeys.Match(route.URI); matched != nil {
				apiKeys = append(apiKeys, matched)
			}

			routes = append(routes, r.NewRouteHandler(aRouter, route))
			if views := router.ExtractCacheableViews(route); len(views) > 0 {
				routes = append(routes, r.NewWarmupRoute(r.routeURL(r.config.APIPrefix, r.config.Meta.CacheWarmURI, route.URI), route))
			}
			routes = append(routes, r.NewViewMetaHandler(r.routeURL(r.config.APIPrefix, r.config.Meta.ViewURI, route.URI), route))
			routes = append(routes, r.NewOpenAPIRoute(r.routeURL(r.config.APIPrefix, r.config.Meta.OpenApiURI, route.URI), route))
			routes = append(routes, r.NewStructRoute(r.routeURL(r.config.APIPrefix, r.config.Meta.StructURI, route.URI), route))

			if len(apiKeys) > 0 {
				for i := routesLen; i < len(routes); i++ {
					routes[i].ApiKeys = apiKeys
					for _, aRoute := range routes[i].Routes {
						aRoute.AddApiKeys(apiKeys...)
					}
				}
			}
		}
	}

	routes = append(routes, r.NewStatusRoute(), r.NewMetricRoute(), r.NewConfigRoute())

	matchables := make([]matcher.Matchable, 0, len(routes))
	routesMetas := make([]*RouteMeta, 0, len(routes))

	for _, route := range routes {
		matchables = append(matchables, route)
		routesMetas = append(routesMetas, &route.RouteMeta)
	}

	for _, route := range routes {
		matched, err := r.apiKeyMatcher.MatchPrefix("", route.URI())
		if err != nil {
			continue
		}

		for _, matchable := range matched {
			apiKeyWrapper, ok := matchable.(*ApiKeyWrapper)
			if !ok {
				continue
			}

			route.ApiKeys = append(route.ApiKeys, apiKeyWrapper.apiKey)
		}
	}

	return matcher.NewMatcher(matchables), routesMetas
}

func (r *Router) MatchAllByPrefix(URL string) []*router.Route {
	matched := r.routeMatcher.MatchAll("", URL)
	var routes []*router.Route

	for _, matchable := range matched {
		route, ok := matchable.(*Route)
		if !ok {
			continue
		}

		routes = append(routes, route.Routes...)
	}

	return routes
}

func (r *Router) interceptIfNeeded(writer http.ResponseWriter, request *http.Request) bool {
	for _, interceptor := range r.interceptors {
		redirected, err := interceptor.Intercept(request)
		if err != nil {
			code, message := httputils.BuildErrorResponse(err)
			write(writer, code, []byte(message))
			return false
		}

		if redirected {
			break
		}
	}

	if r.localInterceptorMatcher != nil {
		matched, err := r.localInterceptorMatcher.MatchPrefix("", request.URL.Path)
		if err == nil {
			response := httputils.NewClosableResponse(writer)
			for _, matchable := range matched {
				matchable.(*Route).Handle(response, request)
				if response.Closed {
					return false
				}
			}
		}
	}

	return true
}

func newLocalInterceptorMatcher(index map[string]*router.Router) *matcher.Matcher {
	matchable := make([]matcher.Matchable, 0)
	for _, aRouter := range index {
		routerInterceptor, ok := aRouter.Interceptor()
		if !ok {
			continue
		}

		matchable = append(matchable, NewInterceptorRoute(aRouter, routerInterceptor))
	}

	if len(matchable) > 0 {
		return matcher.NewMatcher(matchable)
	}

	return nil
}
