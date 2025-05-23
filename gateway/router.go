package gateway

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/viant/afs"
	ahttp "github.com/viant/afs/adapter/http"
	"github.com/viant/afs/storage"
	furl "github.com/viant/afs/url"
	"github.com/viant/cloudless/gateway/matcher"
	"github.com/viant/datly/gateway/router"
	"github.com/viant/datly/gateway/router/openapi/openapi3"
	"github.com/viant/datly/gateway/runtime/meta"
	"github.com/viant/datly/gateway/warmup"
	"github.com/viant/datly/repository"
	"github.com/viant/datly/repository/contract"
	"github.com/viant/datly/repository/path"
	"github.com/viant/datly/service/operator"
	"github.com/viant/datly/service/session"
	"github.com/viant/datly/view"
	vcontext "github.com/viant/datly/view/context"
	"github.com/viant/gmetric"
	serverproto "github.com/viant/mcp-protocol/server"
	"github.com/viant/xdatly/handler/async"
	hstate "github.com/viant/xdatly/handler/state"

	"net/http"
	"net/url"
	"os"
	"strings"
)

type (
	Router struct {
		routeMatcher  *matcher.Matcher
		apiKeyMatcher *matcher.Matcher
		repository    *repository.Service
		operator      *operator.Service
		config        *Config
		OpenAPIInfo   openapi3.Info
		metrics       *gmetric.Service
		statusHandler http.Handler
		paths         []*contract.Path
		mcpRegistry   *serverproto.Registry
	}

	AvailableRoutesError struct {
		Message string
		Paths   []*contract.Path
	}

	ApiKeyWrapper struct {
		apiKey *path.APIKey
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
func NewRouter(ctx context.Context, components *repository.Service, config *Config, metrics *gmetric.Service, statusHandler http.Handler, mcpRegistry *serverproto.Registry) (*Router, error) {
	r := &Router{
		config:        config,
		metrics:       metrics,
		statusHandler: statusHandler,
		repository:    components,
		operator:      operator.New(),
		apiKeyMatcher: newApiKeyMatcher(config.APIKeys),
		mcpRegistry:   mcpRegistry,
	}
	return r, r.init(ctx)
}

func newApiKeyMatcher(keys path.APIKeys) *matcher.Matcher {
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
		r.handleErrorCode(writer, http.StatusInternalServerError, err)
		return
	}
	errStatusCode, err := r.handleRoute(writer, request)
	r.handleErrorCode(writer, errStatusCode, err)
}

func (r *Router) handleRoute(writer http.ResponseWriter, request *http.Request) (int, error) {
	if !meta.IsAuthorized(request, r.config.Meta.AllowedSubnet) {
		return http.StatusForbidden, nil
	}
	aRoute, err := r.match(request.Method, request.URL.Path, request)
	if err != nil {
		return http.StatusNotFound, err
	}
	errorStatusCode := aRoute.Handle(writer, request)
	return errorStatusCode, nil
}

var fs = afs.New()

func (r *Router) DispatchStorageEvent(ctx context.Context, object storage.Object) error {
	data, err := fs.Download(ctx, object)
	if err != nil {
		return err
	}
	job := &async.Job{}
	if err = json.Unmarshal(data, job); err != nil {
		return err
	}
	job.EventURL = object.URL()
	return r.HandleJob(ctx, job)
}

func (r *Router) HandleJob(ctx context.Context, aJob *async.Job) error {
	aPath := &contract.Path{
		URI:    aJob.URI,
		Method: aJob.Method,
	}
	registry := r.repository.Registry()
	aComponent, err := registry.Lookup(ctx, aPath)
	if err != nil {
		return err
	}
	URL, err := url.Parse("http://localhost/" + aPath.URI)
	if err != nil {
		return err
	}
	ctx = vcontext.WithValue(ctx, async.JobKey, aJob)
	ctx = vcontext.WithValue(ctx, async.InvocationTypeKey, async.InvocationTypeEvent)
	request := &http.Request{Method: aJob.Method, URL: URL, RequestURI: aPath.URI}
	unmarshal := aComponent.UnmarshalFunc(request)
	locatorOptions := append(aComponent.LocatorOptions(request, hstate.NewForm(), unmarshal))
	aSession := session.New(aComponent.View,
		session.WithAuth(r.repository.Auth()),
		session.WithComponent(aComponent),
		session.WithLocatorOptions(locatorOptions...),
		session.WithOperate(r.operator.Operate))
	if err != nil {
		return err
	}
	if err = aSession.Unmarshal(aComponent.Input.Type.Parameters, []byte(aJob.State)); err != nil {
		return err
	}
	if _, err = r.operator.Operate(ctx, aSession, aComponent); err != nil {
		return err
	}
	return nil
}

func (r *Router) Match(method, URL string, req *http.Request) (*Route, error) {
	return r.match(method, URL, req)
}

func (r *Router) match(method string, URL string, req *http.Request) (*Route, error) {
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
		//TODO how would we get here ?
		var routes []*contract.Path
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

			routes = append(routes, asRoute.Path)
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

func (r *Router) handleErrorCode(writer http.ResponseWriter, errStatusCode int, err error) {
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

func (r *Router) PreCacheables(ctx context.Context, method string, uri string) ([]*view.View, error) {
	route, err := r.Match(method, uri, nil)
	if err != nil {
		return nil, err
	}
	return r.extractCacheableViews(ctx, route.Providers...)(ctx, method, uri)
}

func (r *Router) availableRoutesErr(statusCode int, err error) error {
	return &HttpError{
		Code: statusCode,
		Err: &AvailableRoutesError{
			Message: err.Error(),
			Paths:   r.paths,
		},
	}
}

func (r *Router) extractCacheableViews(ctx context.Context, providers ...*repository.Provider) warmup.PreCachables {
	return func(ctx context.Context, _, _ string) ([]*view.View, error) {
		var result []*view.View
		for _, provider := range providers {
			aComponent, err := provider.Component(ctx)
			if err != nil {
				return nil, err
			}
			views, err := router.ExtractCacheableViews(ctx, aComponent)
			if err != nil {
				return nil, err
			}
			result = append(result, views...)
		}
		return result, nil
	}
}

func (r *Router) init(ctx context.Context) (err error) {
	r.routeMatcher, r.paths, err = r.newMatcher(ctx)
	return err
}

func (r *Router) newMatcher(ctx context.Context) (*matcher.Matcher, []*contract.Path, error) {
	routes := make([]*Route, 0)
	paths := make([]*contract.Path, 0, len(routes))
	container := r.repository.Container()

	unique := map[string]bool{}
	var openAPIs = map[string][]*repository.Provider{}
	var optionsPaths = map[string][]*path.Path{}
	for _, anItem := range container.Items {
		for _, aPath := range anItem.Paths {
			if aPath.Internal {
				continue
			}
			var apiKeys []*path.APIKey
			paths = append(paths, &aPath.Path)
			if matched := r.config.APIKeys.Match(aPath.URI); matched != nil {
				aPath.APIKey = matched
				apiKeys = append(apiKeys, matched)
			}
			offset := len(routes)

			if aPath.ContentURL != "" {
				routes = append(routes, r.NewContentRoute(aPath)...)
			} else {
				provider, err := r.repository.Registry().LookupProvider(ctx, &aPath.Path)
				if err != nil {
					return nil, nil, fmt.Errorf("failed to locate component provider: %w", err)
				}

				r.EsnureCors(aPath)
				aRoute := r.NewRouteHandler(router.New(aPath, provider, r.repository.Registry(), r.repository.Auth(), r.config.Version, r.config.Logging))
				routes = append(routes, aRoute)
				if aPath.Cors != nil {
					optionsPaths[aPath.URI] = append(optionsPaths[aPath.URI], aPath)
				}

				if !strings.HasPrefix(aPath.URI, r.config.APIPrefix) {
					continue
				}

				//	name := component.View.Ref
				//	if name == "" {
				//		name = component.View.Name
				//	}
				//	description := component.Description
				//	if description == "" {
				//		if aPath.Method == http.MethodGet {
				//			description = "Query data from " + name + " view; source: " + component.View.Source()
				//		} else {
				//			description = "Modify data in " + name + " view; destination: " + component.View.Table
				//		}
				//	}

				if r.mcpRegistry != nil && aPath.HasMCPIntegration() {
					if aPath.MCPResource {
						if err = r.buildResourceIntegration(anItem, aPath, aRoute, provider); err != nil {
							return nil, nil, fmt.Errorf("failed to build resource integration: %w", err)
						}
					}
					if aPath.MCPTemplateResource {
						if err = r.buildTemplateResourceIntegration(anItem, aPath, aRoute, provider); err != nil {
							return nil, nil, fmt.Errorf("failed to build template resource integration: %w", err)
						}
					}
					if aPath.MCPTool {
						if err = r.buildToolsIntegration(anItem, aPath, aRoute, provider); err != nil {
							return nil, nil, fmt.Errorf("failed to build tool integration: %w", err)
						}
					}
				}

				routes = append(routes, r.NewViewMetaHandler(r.routeURL(r.config.Meta.ViewURI, aPath.URI), provider))
				key := r.routeURL(r.config.Meta.OpenApiURI, aPath.URI)
				openAPIs[key] = append(openAPIs[key], provider)

				if !unique[aPath.URI] {
					unique[aPath.URI] = true
					//				//routes = append(routes, r.NewOpenAPIRoute(r.routeURL(r.config.Build.OpenApiURI, aPath.URI), r.repository, provider))
					routes = append(routes, r.NewStructRoute(r.routeURL(r.config.Meta.StructURI, aPath.URI), provider))
					routes = append(routes, r.NewStateRoute(r.routeURL(r.config.Meta.StateURI, aPath.URI), provider))
					routes = append(routes, r.NewMetricRoute(r.metricURL(r.config.Meta.MetricURI, aPath.URI)))
				}

				//TODO extend path.Path with cache info to pre exract cacheable view
				//if views := router.ExtractCacheableViews(route); len(views) > 0 {
				//	routes = append(routes, r.NewWarmupRoute(r.routeURL(r.config.APIPrefix, r.config.Build.CacheWarmURI, route.URI), route))
				//}
			}
			if len(apiKeys) > 0 { //update keys to all path derived routes
				for i := offset; i < len(routes); i++ {
					routes[i].ApiKeys = apiKeys
				}
			}
			// ---
			// ---

		}
	}

	for key, providers := range openAPIs {
		routes = append(routes, r.NewOpenAPIRoute(key, r.repository, providers...))
	}

	for uri, paths := range optionsPaths {
		routes = append(routes, r.NewOptionsRoute(uri, paths))

	}

	if r.statusHandler != nil {
		routes = append(routes, r.NewStatusRoute())
	}

	routes = append(
		routes,
		r.NewConfigRoute(),
	)

	matchables := make([]matcher.Matchable, 0, len(routes))
	for _, route := range routes {
		matchables = append(matchables, route)
	}
	return matcher.NewMatcher(matchables), paths, nil
}
func (r *Router) NewContentRoute(aPath *path.Path) []*Route {
	if !strings.HasSuffix(aPath.Path.URI, "/") && !strings.HasSuffix(aPath.Path.URI, "*") {
		aPath.Path.URI += "/"
	}
	var result []*Route

	pathURI := aPath.Path.URI
	if strings.HasSuffix(pathURI, "/") {
		pathURI = pathURI[:len(pathURI)-1]
	}

	if strings.HasSuffix(pathURI, "/*") {
		pathURI = pathURI[:len(pathURI)-2]
	}

	contentPath := furl.Join(r.config.ContentURL, aPath.ContentURL)
	fileSever := http.FileServer(ahttp.New(afs.New(), contentPath))

	indexContentURL := furl.Join(contentPath, "index.html")
	if _, err := fs.Exists(context.Background(), indexContentURL); err == nil {
		result = r.addIndexRoute(indexContentURL, result)
	}

	defaultRoute := &Route{Path: &aPath.Path,
		MCP:  &aPath.ModelContextProtocol,
		Meta: &aPath.Meta,
		Handler: func(ctx context.Context, response http.ResponseWriter, req *http.Request) {
			request := req.Clone(ctx)
			if index := strings.Index(request.URL.Path, pathURI); index != -1 {
				URI := request.RequestURI[index+len(pathURI):]
				if strings.Index(URI, ".") == -1 {
					URI = furl.Join(URI, "index.html")
				}
				request.URL.Path = URI
				request.RequestURI = request.URL.RequestURI()
			}
			fileSever.ServeHTTP(response, request)
			if aPath.Cors != nil {
				cors := r.EsnureCors(aPath)
				router.CorsHandler(req, cors)(response)
			}
		}}
	result = append(result, defaultRoute)
	aWildcardPath := wildcardPath(aPath)
	route := &Route{Path: &aWildcardPath.Path, Handler: func(ctx context.Context, response http.ResponseWriter, req *http.Request) {
		request := req.Clone(ctx)
		if index := strings.Index(request.URL.Path, pathURI); index != -1 {
			URI := request.RequestURI[index+len(pathURI)+1:]
			request.URL.Path = URI
			request.RequestURI = request.URL.RequestURI()
		}

		fileSever.ServeHTTP(response, request)
		if aPath.Cors != nil {
			cors := r.EsnureCors(aPath)
			router.CorsHandler(req, cors)(response)
		}
	}}
	result = append(result, route)
	return result
}

func (r *Router) addIndexRoute(indexContentURL string, result []*Route) []*Route {
	if content, err := fs.DownloadWithURL(context.Background(), indexContentURL); err == nil {
		result = append(result, &Route{Path: contract.NewPath("GET", "/"), Handler: func(ctx context.Context, response http.ResponseWriter, req *http.Request) {
			response.Header().Set("Content-Type", "text/html")
			response.Write(content)
		}})
		result = append(result, &Route{Path: contract.NewPath("GET", "/index.html"), Handler: func(ctx context.Context, response http.ResponseWriter, req *http.Request) {
			response.Header().Set("Content-Type", "text/html")
			response.Write(content)
		}})
	}
	return result
}

func (r *Router) NewOptionsRoute(uri string, paths []*path.Path) *Route {
	return &Route{
		Path: &contract.Path{
			URI:    uri,
			Method: http.MethodOptions,
		},
		Handler: func(ctx context.Context, response http.ResponseWriter, req *http.Request) {
			cors := r.EsnureCors(paths...)
			router.CorsHandler(req, cors)(response)
		},
	}
}

func (r *Router) EsnureCors(paths ...*path.Path) *path.Cors {
	allowedMethods := []string{}
	cors := &path.Cors{AllowMethods: &allowedMethods}

	for _, aPath := range paths {
		if aPath.Cors == nil {
			aPath.Cors = &path.Cors{}
		}

		if r.config.CORS != nil {
			aPath.Cors.Inherit(r.config.CORS)
		}
		if aPath.Cors.AllowCredentials != nil {
			cors.AllowCredentials = aPath.Cors.AllowCredentials
		}
		if aPath.Cors.AllowHeaders != nil {
			cors.AllowHeaders = aPath.Cors.AllowHeaders
		}
		if aPath.Cors.ExposeHeaders != nil {
			cors.ExposeHeaders = aPath.Cors.ExposeHeaders
		}
		if aPath.Cors.AllowMethods != nil {
			*cors.AllowMethods = append(*cors.AllowMethods, *aPath.Cors.AllowMethods...)
		}
		if aPath.Cors.AllowOrigins != nil {
			cors.AllowOrigins = aPath.Cors.AllowOrigins
		}
		aPath.Cors = cors
	}
	return cors
}

func wildcardPath(aPath *path.Path) *path.Path {
	ret := *aPath
	if !strings.HasSuffix(ret.URI, "*") {
		if !strings.HasSuffix(ret.URI, "/") {
			ret.URI += "/"
		}
		aPath.URI += "*"
	}

	if strings.HasSuffix(ret.URI, "*") {
		ret.URI = ret.URI[:len(ret.URI)-1]
	}

	return &ret
}
