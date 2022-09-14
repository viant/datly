package gateway

import (
	"encoding/json"
	furl "github.com/viant/afs/url"
	"github.com/viant/datly/gateway/runtime/meta"
	"github.com/viant/datly/gateway/warmup"
	"github.com/viant/datly/router"
	"github.com/viant/datly/router/openapi3"
	"github.com/viant/datly/view"
	"github.com/viant/gmetric"
	"gopkg.in/yaml.v3"
	"net/http"
	"net/url"
	"os"
	"strings"
)

//const wildcard = `{DATLY_WILDCARD}`

type (
	Router struct {
		routersIndex    map[string]int
		routers         []*router.Router
		routeMatcher    *router.Matcher
		config          *Config
		prefixesMatcher *router.Matcher
		routes          []*router.Route
		OpenAPIInfo     openapi3.Info
		metrics         *gmetric.Service
		statusHandler   http.Handler
		authorizer      Authorizer
		availableRoutes []Route
		apiKeyMatcher   *router.Matcher
		metaConfig      *meta.Config
	}

	AvailableRoutesError struct {
		Message string
		Routes  []Route
	}

	Route struct {
		Method string
		URL    string
	}

	Prefix struct {
		Indexed string
	}

	ApiKeyWrapper struct {
		Indexed string
		apiKey  *router.APIKey
	}
)

func (a *ApiKeyWrapper) HttpURI() string {
	return a.Indexed
}

func (a *ApiKeyWrapper) HttpMethod() string {
	return ""
}

func (a *ApiKeyWrapper) CorsEnabled() bool {
	return false
}

func (a *AvailableRoutesError) Error() string {
	marshal, _ := json.Marshal(a)
	return string(marshal)
}

func (p *Prefix) HttpURI() string {
	return p.Indexed
}

func (p *Prefix) HttpMethod() string {
	return ""
}

func (p *Prefix) CorsEnabled() bool {
	return false
}

//NewRouter creates new router
//TODO: http handlers can be chosen by matcher. We can create wrapper for router.Matchable that will handle the request using Route/Routes etc.
func NewRouter(routersIndex map[string]*router.Router, config *Config, metrics *gmetric.Service, statusHandler http.Handler, authorizer Authorizer) *Router {
	routers := asRouterSlice(routersIndex)
	matcher, routes, index := newMatcher(routers)

	var metaConfig meta.Config
	if config != nil {
		metaConfig = config.Meta
		metaConfig.MetricURI = router.AsRelative(metaConfig.MetricURI)
		metaConfig.ViewURI = router.AsRelative(metaConfig.ViewURI)
		metaConfig.OpenApiURI = router.AsRelative(metaConfig.OpenApiURI)
		metaConfig.StatusURI = router.AsRelative(metaConfig.StatusURI)
		metaConfig.CacheWarmURI = router.AsRelative(metaConfig.CacheWarmURI)
	}

	return &Router{
		routersIndex:  index,
		routers:       routers,
		routeMatcher:  matcher,
		config:        config,
		routes:        routes,
		metrics:       metrics,
		statusHandler: statusHandler,
		prefixesMatcher: newPrefixMatcher([]string{
			metaConfig.ViewURI,
			metaConfig.MetricURI,
			metaConfig.StatusURI,
			metaConfig.CacheWarmURI,
			metaConfig.OpenApiURI,
			metaConfig.ConfigURI,
			config.APIPrefix,
		}),
		authorizer:      authorizer,
		availableRoutes: asAvailableRoutes(routes),
		apiKeyMatcher:   newApiKeyMatcher(config.APIKeys),
		metaConfig:      &metaConfig,
	}
}

func newApiKeyMatcher(keys router.APIKeys) *router.Matcher {
	if len(keys) == 0 {
		return nil
	}

	matchables := make([]router.Matchable, 0, len(keys))
	for i := range keys {
		matchables = append(matchables,
			&ApiKeyWrapper{
				Indexed: keys[i].URI,
				apiKey:  keys[i],
			},
		)
	}

	return router.NewMatcher(matchables)
}

//TODO: eagerly with ApiKey check
func asAvailableRoutes(routes []*router.Route) []Route {
	result := make([]Route, len(routes))
	for i, route := range routes {
		result[i] = Route{
			Method: route.Method,
			URL:    route.URI,
		}
	}

	return result
}

func newPrefixMatcher(prefixes []string) *router.Matcher {
	matchables := make([]router.Matchable, 0, len(prefixes))
	for _, prefix := range prefixes {
		if prefix == "" {
			continue
		}

		matchables = append(matchables, newPrefix(prefix))
	}

	return router.NewMatcher(matchables)
}

func newPrefix(prefix string) router.Matchable {
	indexed := prefix

	return &Prefix{
		Indexed: router.AsRelative(indexed),
	}
}

func (r *Router) Handle(writer http.ResponseWriter, request *http.Request) {
	err := r.ensureRequestURL(request)
	if err != nil {
		r.handleErrIfNeeded(writer, http.StatusInternalServerError, err)
		return
	}

	if !r.authorizeRequestIfNeeded(writer, request) {
		return
	}

	errStatusCode, err := r.handle(writer, request)
	r.handleErrIfNeeded(writer, errStatusCode, err)
}

func (r *Router) handle(writer http.ResponseWriter, request *http.Request) (int, error) {
	urlPath := request.URL.Path
	actualPrefix, viewPath := r.asAPIPrefix(urlPath)

	if (actualPrefix != r.config.APIPrefix && !meta.IsAuthorized(request, r.config.Meta.AllowedSubnet)) || !r.apiKeyMatches(urlPath, request) || !r.apiKeyMatches(viewPath, request) {
		return http.StatusForbidden, nil
	}

	switch actualPrefix {
	case r.metaConfig.MetricURI:
		r.handleMetrics(writer, request)
		return http.StatusOK, nil
	case r.metaConfig.ConfigURI:
		r.handleConfig(writer)
		return http.StatusOK, nil
	case r.metaConfig.OpenApiURI:
		return r.matchByMultiRoutes(writer, request, viewPath)
	case r.metaConfig.StatusURI:
		if r.statusHandler == nil {
			return http.StatusNotFound, nil
		}

		r.statusHandler.ServeHTTP(writer, request)
		return http.StatusOK, nil
	default:
		return r.matchByRoute(writer, request, viewPath, actualPrefix)
	}
}

func (r *Router) matchByRoute(writer http.ResponseWriter, request *http.Request, viewPath string, actualPrefix string) (int, error) {
	aRoute, aRouter, err := r.Match(request.Method, viewPath)
	if err != nil {
		return http.StatusNotFound, r.availableRoutesErr(err)
	}

	if !r.apiKeyMatches(aRoute.URI, request) {
		return http.StatusForbidden, nil
	}

	return r.handleRouteWithPrefix(writer, request, actualPrefix, aRouter, aRoute)
}

func (r *Router) handleRouteWithPrefix(writer http.ResponseWriter, request *http.Request, actualPrefix string, aRouter *router.Router, aRoute *router.Route) (int, error) {
	switch actualPrefix {
	case r.metaConfig.ViewURI:
		r.handleView(writer, aRoute)
		return http.StatusOK, nil
	case r.metaConfig.CacheWarmURI:
		r.handleCacheWarmup(writer, request, aRoute)
		return http.StatusOK, nil
	default:
		return r.handleRoute(writer, request, aRouter, aRoute)
	}
}

func (r *Router) handleRoute(writer http.ResponseWriter, request *http.Request, aRouter *router.Router, aRoute *router.Route) (int, error) {
	if err := aRouter.HandleRoute(writer, request, aRoute); err != nil {
		return http.StatusNotFound, err
	}

	return http.StatusOK, nil
}

func (r *Router) apiKeyMatches(routePath string, request *http.Request) bool {
	if r.apiKeyMatcher == nil {
		return true
	}

	matched, err := r.apiKeyMatcher.MatchPrefix("", routePath)
	if err != nil || len(matched) == 0 {
		return true
	}

	var apiKey *router.APIKey
	var numOfSegments int

	for _, matchable := range matched {
		switch actual := matchable.(type) {
		case *ApiKeyWrapper:
			candidateSegments := strings.Count(router.AsRelative(actual.apiKey.URI), "/")
			if apiKey == nil || candidateSegments > numOfSegments {
				apiKey = actual.apiKey
				numOfSegments = candidateSegments
			}
		}
	}

	if apiKey == nil {
		return true
	}

	return request.Header.Get(apiKey.Header) == apiKey.Value
}

func (r *Router) Match(method, URL string) (*router.Route, *router.Router, error) {
	route, err := r.routeMatcher.MatchOneRoute(method, URL)
	if err != nil {
		return nil, nil, err
	}

	return route, r.routers[r.routersIndex[combine(route.Method, route.URI)]], nil
}

func (r *Router) asAPIPrefix(URIPath string) (prefix string, path string) {
	URIPath = router.AsRelative(URIPath)
	matched, err := r.prefixesMatcher.MatchPrefix("", URIPath)
	if err != nil {
		return r.config.APIPrefix, URIPath
	}

	var matchedPrefix *Prefix
	numOfSegments := 0

	for _, matchable := range matched {
		asPrefix, ok := matchable.(*Prefix)
		if !ok {
			continue
		}

		candidateSegmentsCount := strings.Count(asPrefix.Indexed, "/")
		if matchedPrefix == nil || numOfSegments < candidateSegmentsCount {
			matchedPrefix = asPrefix
			numOfSegments = candidateSegmentsCount
		}
	}

	if matchedPrefix == nil {
		return r.config.APIPrefix, URIPath
	}

	if matchedPrefix.Indexed != matchedPrefix.Indexed {
		URIPath = URIPath[strings.Index(URIPath, "/")+1:]
	}

	return matchedPrefix.Indexed, strings.Replace(URIPath, matchedPrefix.Indexed, router.AsRelative(r.config.APIPrefix), 1)
}

func (r *Router) handleView(writer http.ResponseWriter, route *router.Route) {
	errStatusCode, err := r.handleViewWithErr(writer, route)
	r.handleErrIfNeeded(writer, errStatusCode, err)
}

func (r *Router) handleViewWithErr(writer http.ResponseWriter, route *router.Route) (int, error) {
	JSON, err := json.Marshal(route.View)
	if err != nil {
		return http.StatusInternalServerError, err
	}

	transient := map[string]interface{}{}
	err = json.Unmarshal(JSON, &transient)
	if err != nil {
		return http.StatusInternalServerError, err
	}

	YAML, err := yaml.Marshal(transient)
	if err != nil {
		return http.StatusInternalServerError, err
	}

	writer.Header().Set("Content-Type", "text/yaml")
	writer.Write(YAML)
	return http.StatusOK, nil
}

func (r *Router) matchByMultiRoutes(writer http.ResponseWriter, request *http.Request, path string) (int, error) {
	allMatched := r.MatchAll(path)

	var allowed []*router.Route
	for i, route := range allMatched {
		if !r.apiKeyMatches(route.URI, request) {
			continue
		}

		allowed = append(allowed, allMatched[i])
	}

	if len(allowed) == 0 && len(allMatched) != 0 {
		return http.StatusForbidden, nil
	}

	r.handleOpenAPI(writer, allMatched)
	return http.StatusOK, nil
}

func (r *Router) handleOpenAPI(writer http.ResponseWriter, routes []*router.Route) {
	statusCode, err := r.handleOpenAPIWithErr(writer, routes)
	r.handleErrIfNeeded(writer, statusCode, err)
}

func (r *Router) handleOpenAPIWithErr(writer http.ResponseWriter, routes []*router.Route) (int, error) {
	spec, err := router.GenerateOpenAPI3Spec(r.OpenAPIInfo, routes...)
	if err != nil {
		return http.StatusInternalServerError, err
	}

	specMarshal, err := yaml.Marshal(spec)
	if err != nil {
		return http.StatusInternalServerError, err
	}

	writer.Header().Set("Content-Type", "text/yaml")
	writer.Write(specMarshal)
	writer.WriteHeader(http.StatusOK)
	return http.StatusOK, nil
}

func (r *Router) MatchAll(path string) []*router.Route {
	return r.matchAll("", path)
}

func (r *Router) MatchAllWithMethod(method, path string) []*router.Route {
	return r.matchAll(method, path)
}

func (r *Router) matchAll(method, path string) []*router.Route {
	if path == r.metaConfig.OpenApiURI {
		return r.routes
	}

	if path == "" && method == "" {
		return r.routes
	}

	matched, _ := r.routeMatcher.MatchAllRoutes("", path)
	return matched
}

func (r *Router) handleErrIfNeeded(writer http.ResponseWriter, errStatusCode int, err error) {
	if errStatusCode >= http.StatusBadRequest {
		writer.WriteHeader(errStatusCode)
		if err != nil {
			writer.Write([]byte(err.Error()))
		}
	}
}

func (r *Router) handleMetrics(writer http.ResponseWriter, req *http.Request) {
	gmetric.NewHandler(r.metaConfig.MetricURI, r.metrics).ServeHTTP(writer, req)
}

func (r *Router) handleConfig(writer http.ResponseWriter) {
	statusCode, err := r.handleConfigWithErr(writer)
	r.handleErrIfNeeded(writer, statusCode, err)
}

func (r *Router) handleConfigWithErr(writer http.ResponseWriter) (int, error) {
	JSON, err := json.Marshal(r.config)
	if err != nil {
		return http.StatusInternalServerError, err
	}
	writer.Header().Set("Content-Type", "application/json")
	writer.Write(JSON)
	return http.StatusOK, nil
}

func (r *Router) handleCacheWarmup(writer http.ResponseWriter, request *http.Request, route *router.Route) {
	statusCode, err := r.handleCacheWarmupWithErr(writer, request, route)
	r.handleErrIfNeeded(writer, statusCode, err)
}

func (r *Router) handleCacheWarmupWithErr(writer http.ResponseWriter, request *http.Request, route *router.Route) (int, error) {
	response := warmup.PreCache(r.extractCacheableViews(route), route.URI)
	data, err := json.Marshal(response)

	if err != nil {
		return http.StatusInternalServerError, err
	}

	writer.Write(data)
	return http.StatusOK, nil
}

func (r *Router) extractCacheableViews(route *router.Route) warmup.PreCachables {
	var views []*view.View
	appendCacheWarmupViews(route.View, &views)

	return func(_, _ string) ([]*view.View, error) {
		return views, nil
	}
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
	route, _, err := r.Match(method, uri)
	if err != nil {
		return nil, err
	}

	return r.extractCacheableViews(route)(method, uri)
}

func (r *Router) availableRoutesErr(err error) error {
	return &AvailableRoutesError{
		Message: err.Error(),
		Routes:  r.availableRoutes,
	}
}

func appendCacheWarmupViews(aView *view.View, result *[]*view.View) {
	if aCache := aView.Cache; aCache != nil && aCache.Warmup != nil {
		*result = append(*result, aView)
	}
	if len(aView.With) == 0 {
		return
	}
	for i := range aView.With {
		appendCacheWarmupViews(&aView.With[i].Of.View, result)
	}
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

func newMatcher(routers []*router.Router) (*router.Matcher, []*router.Route, map[string]int) {
	routesSize := 0
	for _, r := range routers {
		routesSize += len(r.Routes(""))
	}

	routes, routerIndex := make([]*router.Route, routesSize), map[string]int{}

	counter := 0
	for i, aRouter := range routers {
		routerRoutes := aRouter.Routes("")
		for routeIndex, route := range routerRoutes {
			routes[counter] = routerRoutes[routeIndex]
			routerIndex[combine(route.Method, route.URI)] = i
			counter++
		}
	}

	return router.NewRouteMatcher(routes), routes, routerIndex
}

func combine(method string, uri string) string {
	return method + ":///" + uri
}
