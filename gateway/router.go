package gateway

import (
	"context"
	"encoding/json"
	"fmt"
	furl "github.com/viant/afs/url"
	"github.com/viant/cloudless/gateway/matcher"
	"github.com/viant/datly/gateway/router"
	"github.com/viant/datly/gateway/router/openapi3"
	"github.com/viant/datly/gateway/runtime/meta"
	"github.com/viant/datly/gateway/warmup"
	"github.com/viant/datly/repository"
	"github.com/viant/datly/repository/component"
	"github.com/viant/datly/repository/locator/component/dispatcher"
	sdispatcher "github.com/viant/datly/service/dispatcher"
	"github.com/viant/datly/service/session"
	httputils2 "github.com/viant/datly/utils/httputils"
	"github.com/viant/datly/view"
	"github.com/viant/gmetric"
	"github.com/viant/xdatly/handler/async"
	http2 "github.com/viant/xdatly/handler/http"
	"net/http"
	"net/url"
	"os"
	"strings"
)

type (
	Router struct {
		RouterOptions

		routeMatcher            *matcher.Matcher
		localInterceptorMatcher *matcher.Matcher
		apiKeyMatcher           *matcher.Matcher
		OpenAPIInfo             openapi3.Info
		routes                  []*RouteMeta
		namedRoutes             map[string]*router.Route
		registry                *repository.Registry
		dispatcher              component.Dispatcher
		dispatcherService       *sdispatcher.Service
	}

	AvailableRoutesError struct {
		Message string
		Routes  []*RouteMeta
	}

	ApiKeyWrapper struct {
		apiKey *router.APIKey
	}

	RouterOptions struct {
		Routers       map[string]*router.Router
		LazyRoutes    map[string]*LazyRouterContract
		Config        *Config
		Metrics       *gmetric.Service
		StatusHandler http.Handler
		Authorizer    Authorizer
		Interceptors  []*router.RouteInterceptor
		NewRouterFn   NewRouterFn
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

// NewRouter creates new router
func NewRouter(options RouterOptions) (*Router, error) {
	r := &Router{
		RouterOptions:           options,
		apiKeyMatcher:           newApiKeyMatcher(options.Config.APIKeys),
		localInterceptorMatcher: newLocalInterceptorMatcher(options.Routers),
	}

	return r, r.init()
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
	r.handle(writer, request)
}

func (r *Router) handle(writer http.ResponseWriter, request *http.Request) {
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

	errStatusCode, err := r.handleWithError(writer, request, r.routeMatcher)

	r.handleErrIfNeeded(writer, errStatusCode, err)
}

func (r *Router) handleWithError(writer http.ResponseWriter, request *http.Request, matcher *matcher.Matcher) (int, error) {
	if !meta.IsAuthorized(request, r.Config.Meta.AllowedSubnet) {
		return http.StatusForbidden, nil
	}

	aRoute, err := r.match(matcher, request.Method, request.URL.Path, request)
	if err != nil {
		return http.StatusNotFound, err
	}

	aRoute.Handle(writer, request)

	return http.StatusOK, nil
}

func (r *Router) HandleJob(ctx context.Context, job *async.Job) error {
	path := &component.Path{
		URI:    job.URI,
		Method: job.Method,
	}
	aComponent, err := r.registry.Lookup(path)
	if err != nil {
		return err
	}
	URL, err := url.Parse("http://localhost/" + path.URI)
	if err != nil {
		return err
	}
	ctx = context.WithValue(ctx, async.JobKey, job)
	ctx = context.WithValue(ctx, async.InvocationTypeKey, async.InvocationTypeEvent)
	request := &http.Request{Method: job.Method, URL: URL, RequestURI: path.URI}
	unmarshal := aComponent.UnmarshalFunc(request)
	locatorOptions := append(aComponent.LocatorOptions(request, unmarshal))
	aSession := session.New(aComponent.View, session.WithLocatorOptions(locatorOptions...))
	if err != nil {
		return err
	}
	if err = aSession.Unmarshal(aComponent.Input.Type.Parameters, []byte(job.State)); err != nil {
		return err
	}
	if _, err = r.dispatcherService.Dispatch(ctx, aComponent, aSession); err != nil {
		return err
	}
	return nil
}

func (r *Router) Match(method, URL string, req *http.Request) (*Route, error) {
	return r.match(r.routeMatcher, method, URL, req)
}

func (r *Router) match(matcher *matcher.Matcher, method string, URL string, req *http.Request) (*Route, error) {
	matched := matcher.MatchAll(method, URL)
	switch len(matched) {
	case 0:
		return nil, r.availableRoutesErr(http.StatusNotFound, fmt.Errorf("not found route with Method: %v and URL: %v", method, URL))
	case 1:
		return r.asRoute(matched[0])

	default:
		var routes []*router.Route
		var lastMatched *Route
		for _, matchable := range matched {
			asRoute, err := r.asRoute(matchable)
			if err != nil {
				return nil, err
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
			return nil, r.availableRoutesErr(http.StatusForbidden, fmt.Errorf("forbidden"))
		}

		return lastMatched.NewMultiRoute(routes), nil
	}
}

func (r *Router) unexpectedType(asRoute *Route, expected interface{}) error {
	return fmt.Errorf("unexpected Matchable type, wanted %T got %T", asRoute, expected)
}

func (r *Router) handleErrIfNeeded(writer http.ResponseWriter, errStatusCode int, err error) {
	if errStatusCode < http.StatusBadRequest {
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
	if r.Authorizer == nil {
		return true
	}

	return r.Authorizer.Authorize(writer, request)
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

func (r *Router) init() error {
	routers := asRouterSlice(r.Routers)
	r.registry = repository.NewRegistry(r.Config.APIPrefix)
	r.dispatcherService = sdispatcher.New()
	r.dispatcher = dispatcher.New(r.registry)
	r.routeMatcher, r.routes = r.newMatcher(routers)
	r.namedRoutes = map[string]*router.Route{}
	for _, aRouter := range routers {
		routes := aRouter.Routes("")
		for _, route := range routes {
			if route.Name == "" {
				continue
			}

			foundRoute, ok := r.namedRoutes[route.Name]
			if ok {
				return fmt.Errorf("route with %v name already exists under %v, %v", route.Name, foundRoute.Method, foundRoute.URI)
			}
			r.namedRoutes[route.Name] = route
		}
	}
	return nil
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

	var components = make([]*repository.Component, 0, 3*len(routers))
	for _, aRouter := range routers {
		routerRoutes := aRouter.Routes("")
		for _, route := range routerRoutes {
			components = append(components, &route.Component)
			route.SetDispatcher(r.dispatcher)

			routesLen := len(routes)
			var apiKeys []*router.APIKey
			if matched := r.Config.APIKeys.Match(route.URI); matched != nil {
				apiKeys = append(apiKeys, matched)
			}

			routes = append(routes, r.NewRouteHandler(aRouter, route))

			if views := router.ExtractCacheableViews(route); len(views) > 0 {
				routes = append(routes, r.newWarmupRoute(route))
			}

			routes = append(routes, r.newViewMetaRoute(route))
			routes = append(routes, r.newOpenAPIRoute(route))
			routes = append(routes, r.newStructRoute(route))

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

	r.registry.Register(components...)

	routes = append(
		routes,
		r.NewStatusRoute(),
		r.NewMetricRoute(),
		r.NewConfigRoute(),
	)

	matchables := make([]matcher.Matchable, 0, len(routes))
	routesMetas := make([]*RouteMeta, 0, len(routes))

	for _, route := range routes {
		matchables = append(matchables, route)
		routesMetas = append(routesMetas, &route.RouteMeta)
	}

	for _, route := range routes {
		keys := r.apiKeys(route.URI())
		for _, key := range keys {
			route.ApiKeys = append(route.ApiKeys, key.apiKey)
		}
	}

	for _, contract := range r.RouterOptions.LazyRoutes {
		loader := NewRouterLoader(contract.RouterURI, r.RouterOptions.NewRouterFn)
		for _, route := range contract.Routes {
			routeLoader := NewRouteLoader(*route, loader)
			routeHandler := NewLazyRoute(RouteMeta{
				Method: route.Method,
				URL:    route.URI,
			}, routeLoader, func(ctx context.Context, router *router.Router, route *router.Route) (*Route, error) {
				return r.NewRouteHandler(router, route), nil
			}, r.apiKeys)

			viewMetaHandler := NewLazyRoute(r.viewMetaURI(route.URI), routeLoader, func(ctx context.Context, router *router.Router, route *router.Route) (*Route, error) {
				return r.newViewMetaRoute(route), nil
			}, r.apiKeys)

			openAPIHandler := NewLazyRoute(r.openAPIRouteMeta(route.URI), routeLoader, func(ctx context.Context, router *router.Router, route *router.Route) (*Route, error) {
				return r.newOpenAPIRoute(route), nil
			}, r.apiKeys)

			structHandler := NewLazyRoute(r.structRouteMeta(route.URI), routeLoader, func(ctx context.Context, router *router.Router, route *router.Route) (*Route, error) {
				return r.newStructRoute(route), nil
			}, r.apiKeys)

			lazyRoutes := []*LazyRoute{
				routeHandler,
				viewMetaHandler,
				openAPIHandler,
				structHandler,
			}

			if route.Warmup {
				warmupHandler := NewLazyRoute(r.warmupRouteMeta(route.URI), routeLoader, func(ctx context.Context, router *router.Router, route *router.Route) (*Route, error) {
					return r.newWarmupRoute(route), nil
				}, r.apiKeys)

				lazyRoutes = append(lazyRoutes, warmupHandler)
			}

			for _, lazyRoute := range lazyRoutes {
				routesMetas = append(routesMetas, &lazyRoute.RouteMeta)
			}

			for _, lazyRoute := range lazyRoutes {
				matchables = append(matchables, lazyRoute)
			}
		}

		routesMetas = append(routesMetas)
	}

	return matcher.NewMatcher(matchables), routesMetas
}

func (r *Router) newStructRoute(route *router.Route) *Route {
	return r.NewStructRoute(r.structRouteMeta(route.URI), route)
}

func (r *Router) structRouteMeta(URI string) RouteMeta {
	return RouteMeta{
		Method: http.MethodGet,
		URL:    URI,
	}
}

func (r *Router) newOpenAPIRoute(route *router.Route) *Route {
	return r.NewOpenAPIRoute(r.openAPIRouteMeta(route.URI), route)
}

func (r *Router) openAPIRouteMeta(URI string) RouteMeta {
	return RouteMeta{
		Method: http.MethodGet,
		URL:    r.routeURL(r.Config.APIPrefix, r.Config.Meta.OpenApiURI, URI),
	}
}

func (r *Router) newViewMetaRoute(route *router.Route) *Route {
	return r.NewViewMetaHandler(r.viewMetaURI(route.URI), route)
}

func (r *Router) viewMetaURI(URI string) RouteMeta {
	return RouteMeta{
		Method: http.MethodGet,
		URL:    r.routeURL(r.Config.APIPrefix, r.Config.Meta.ViewURI, URI),
	}
}

func (r *Router) newWarmupRoute(route *router.Route) *Route {
	return r.NewWarmupRoute(r.warmupRouteMeta(route.URI), route)
}

func (r *Router) warmupRouteMeta(URI string) RouteMeta {
	return RouteMeta{
		Method: http.MethodPost,
		URL:    r.routeURL(r.Config.APIPrefix, r.Config.Meta.CacheWarmURI, URI),
	}
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
	for _, interceptor := range r.Interceptors {
		redirected, err := interceptor.Intercept(request)
		if err != nil {
			code, message := httputils2.BuildErrorResponse(err)
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
			response := httputils2.NewClosableResponse(writer)
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

func (r *Router) routeLookup(route *http2.Route) (*router.Route, error) {
	if route.Name != "" {
		foundRoute, ok := r.namedRoutes[route.Name]
		if !ok {
			return nil, fmt.Errorf("not found route with name %v", route.Name)
		}

		return foundRoute, nil
	}

	one, err := r.routeMatcher.MatchOne(route.Method, route.URL)
	if err != nil {
		return nil, err
	}

	asRoute := one.(*Route)
	if len(asRoute.Routes) != 1 {
		return nil, fmt.Errorf("not found %v route URL %v", route.Method, route.URL)
	}

	return asRoute.Routes[0], nil
}

func (r *Router) asRoute(matchable matcher.Matchable) (*Route, error) {
	switch actual := matchable.(type) {
	case *Route:
		return actual, nil
	case *LazyRoute:
		return actual.Route(context.Background())
	default:
		return nil, r.unexpectedType(&Route{}, matchable)
	}
}

func (r *Router) apiKeys(URI string) (result []*ApiKeyWrapper) {
	apiKeys, err := r.apiKeyMatcher.MatchPrefix("", URI)
	if err != nil {
		return result
	}

	for _, matched := range apiKeys {
		switch actual := matched.(type) {
		case *ApiKeyWrapper:
			result = append(result, actual)
		}
	}

	return result
}
