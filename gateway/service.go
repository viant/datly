package gateway

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/viant/afs"
	"github.com/viant/afs/cache"
	"github.com/viant/afs/file"
	"github.com/viant/afs/matcher"
	"github.com/viant/afs/option"
	furl "github.com/viant/afs/url"
	"github.com/viant/cloudless/resource"
	"github.com/viant/datly/cmd/env"
	"github.com/viant/datly/service/auth/secret"
	"github.com/viant/datly/utils/httputils"
	pbuild "github.com/viant/pgo/build"
	async2 "github.com/viant/xdatly/handler/async"
	"sync/atomic"

	"github.com/viant/datly/config"
	"github.com/viant/datly/gateway/router"
	"github.com/viant/datly/shared"
	"github.com/viant/datly/view"
	"github.com/viant/gmetric"
	"github.com/viant/pgo/manager"
	"github.com/viant/scy/auth/jwt/signer"
	"net/http"
	"path"
	"strings"
	"sync"
	"time"
)

const (
	pluginsFolder = ".plugins_snapshot"
	metaFolder    = ".meta"
)

var unindexedFolders = []string{pluginsFolder, metaFolder}

type (
	Service struct {
		Config            *Config
		routersIndex      map[string]*router.Router
		fs                afs.Service
		routeTracker      *Tracker
		dependencyTracker *Tracker
		pluginTracker     *Tracker
		assetsTracker     *Tracker

		dataResourcesIndex map[string]*view.Resource
		metrics            *gmetric.Service
		mainRouter         *Router
		cancelFn           context.CancelFunc
		changeSession      *Session
		JWTSigner          *signer.Service
		mux                sync.RWMutex
		configRegistry     *config.Registry
		pluginManager      *manager.Service
		statusHandler      http.Handler
		authorizer         Authorizer

		interceptors router.RouterInterceptors
		nextCheck    time.Time
		isBuilding   int64
	}

	routerConfig struct {
		changed      bool
		resources    map[string]*view.Resource
		interceptors router.RouterInterceptors
	}
)

func (r *Service) ServeHTTP(writer http.ResponseWriter, request *http.Request) {
	aRouter, writer, ok := r.router(writer)
	if !ok {
		return
	}

	aRouter.Handle(writer, request)
}

func (r *Service) router(writer http.ResponseWriter) (*Router, http.ResponseWriter, bool) {
	aRouter, ok := r.Router()
	if !ok {
		writer.WriteHeader(http.StatusNotFound)
		return nil, nil, false
	}

	writer = r.WrapResponseIfNeeded(writer)
	return aRouter, writer, true
}

func (r *Service) Router() (*Router, bool) {
	if err := r.syncChangesIfNeeded(context.Background(), r.metrics, r.statusHandler, r.authorizer, false); err != nil {
		fmt.Printf("[ERROR] failed to sync changes: %v\n", err)
	}

	mainRouter := r.mainRouter
	return mainRouter, mainRouter != nil
}

func (r *Service) Close() error {
	if r.cancelFn != nil {
		r.cancelFn()
	}

	return nil
}

// New creates gateway Service. It is important to call Service.Close before Service got Garbage collected.
// TODO: add lazy routes. I think it should go through the Service.Session.RouterChanges
// TODO: simplify reloading with sync.Once
func New(ctx context.Context, aConfig *Config, statusHandler http.Handler, authorizer Authorizer, registry *config.Registry, metrics *gmetric.Service) (*Service, error) {
	start := time.Now()
	if err := aConfig.Init(); err != nil {
		return nil, err
	}

	err := aConfig.Validate()
	if err != nil {
		return nil, err
	}

	fs, err := newFileService(aConfig)
	if err != nil {
		return nil, err
	}
	syncTime := time.Duration(aConfig.SyncFrequencyMs) * time.Millisecond

	srv := &Service{
		metrics:            metrics,
		configRegistry:     registry,
		Config:             aConfig,
		mux:                sync.RWMutex{},
		fs:                 fs,
		pluginManager:      manager.New(pbuild.NewSequenceChangeNumber(env.BuildTime)),
		dataResourcesIndex: map[string]*view.Resource{},
		routeTracker: NewNotifier(
			aConfig.RouteURL,
			fs,
			syncTime,
		),
		dependencyTracker: NewNotifier(
			aConfig.DependencyURL,
			fs,
			syncTime,
		),
		pluginTracker: NewNotifier(
			aConfig.PluginsURL,
			fs,
			syncTime,
		),
		assetsTracker: NewNotifier(
			aConfig.AssetsURL,
			fs,
			syncTime,
		),
		routersIndex:  map[string]*router.Router{},
		changeSession: NewSession(aConfig.ChangeDetection, map[string]*router.Router{}),
		statusHandler: statusHandler,
		authorizer:    authorizer,
		interceptors:  router.RouterInterceptors{},
	}

	if aConfig.JwtSigner != nil {
		srv.JWTSigner = signer.New(aConfig.JwtSigner)
		if err = srv.JWTSigner.Init(context.Background()); err != nil {
			return nil, err
		}
	}

	if err = initSecrets(ctx, aConfig); err != nil {
		return nil, err
	}

	err = srv.syncChangesIfNeeded(ctx, metrics, statusHandler, authorizer, true)
	fmt.Printf("[INFO] initialised datly: %s\n", time.Now().Sub(start))
	return srv, err
}

const (
	PackageFile = "datly.pkg"
)

func newFileService(aConfig *Config) (afs.Service, error) {
	if !aConfig.UseCacheFS {
		return afs.New(), nil
	}
	URL, err := CommonURL(aConfig.DependencyURL, aConfig.PluginsURL, aConfig.RouteURL)
	if err != nil {
		return nil, err
	}
	return NewCacheFs(URL), nil
}

func NewCacheFs(URL string) afs.Service {
	return cache.Singleton(URL,
		matcher.WithExtExclusion(".so", "so", ".gz", "gz"),
		option.WithCache(PackageFile, "gzip"),
		option.WithLogger(func(format string, args ...interface{}) {
			fmt.Printf(format, args...)
		}))
}

func CommonURL(URLs ...string) (string, error) {
	counter := map[string]int{}
	var base string
	for i, URL := range URLs {
		dir, aPath := furl.Split(URL, "file")
		if base == "" {
			base = dir
		} else {
			if base != dir {
				return base, nil
			}
		}

		URLs[i] = aPath
	}

	for {
		allExhausted := true

		for i, URL := range URLs {
			if len(URL) <= 1 {
				continue
			}

			allExhausted = false
			commonCounter := counter[URL] + 1
			if commonCounter == len(URLs) {
				return furl.Join(base, URL), nil
			}

			counter[URL] = commonCounter
			URLs[i] = path.Dir(URL)
		}

		if allExhausted {
			break
		}
	}

	return base, nil
}

func (r *Service) syncChangesIfNeeded(ctx context.Context, metrics *gmetric.Service, statusHandler http.Handler, authorizer Authorizer, isFirst bool) error {
	started := time.Now()
	if started.Before(r.nextCheck) {
		return nil
	}

	if !atomic.CompareAndSwapInt64(&r.isBuilding, r.isBuilding, 1) {
		return nil
	}

	defer func() {
		r.mux.Lock()
		defer r.mux.Unlock()

		r.nextCheck = time.Now().Add(r.Config.ChangeDetection._retry)
		r.isBuilding = 0

		if r.changeSession == nil {
			return
		}
		r.changeSession.UpdateFailureCounter()
	}()

	r.mux.Lock()
	if r.changeSession == nil {
		r.changeSession = NewSession(r.Config.ChangeDetection, r.routersIndex)
	}
	r.mux.Unlock()

	aRouterConfig, err := r.getDataResources(ctx, r.fs)
	if err != nil {
		return err
	}

	routers, changed, err := r.getRouters(ctx, r.fs, aRouterConfig.resources, aRouterConfig.changed, isFirst)
	if err != nil || !changed {
		return err
	}

	if !isFirst {
		fmt.Printf("[INFO] routers rebuild completed after: %s\n", time.Since(started))
	}

	mainRouter, err := NewRouter(RouterOptions{
		Routers:       routers,
		LazyRoutes:    r.changeSession.LazyRoutes,
		Config:        r.Config,
		Metrics:       metrics,
		StatusHandler: r.statusHandler,
		Authorizer:    authorizer,
		Interceptors:  aRouterConfig.interceptors.AsSlice(),
		NewRouterFn:   r.routerProvider(),
	})

	if err != nil {
		return err
	}

	r.mux.Lock()
	r.mainRouter = mainRouter
	r.routersIndex = routers
	r.dataResourcesIndex = aRouterConfig.resources
	r.interceptors = aRouterConfig.interceptors
	r.changeSession = nil
	r.mux.Unlock()

	return nil
}

func (r *Service) getRouters(ctx context.Context, fs afs.Service, resources map[string]*view.Resource, viewResourcesChanged bool, isFirst bool) (routers map[string]*router.Router, changed bool, err error) {
	if err = r.detectRoutersChanges(ctx, fs); err != nil {
		return nil, false, err
	}

	if !viewResourcesChanged && !r.changeSession.routerChanges.Changed() {
		return r.routersIndex, false, nil
	}

	if !isFirst {
		fmt.Printf("[INFO] detected resources changes, rebuilding routers\n")
	}

	return r.rebuildRouters(ctx, resources, routers, changed)
}

func (r *Service) rebuildRouters(ctx context.Context, resources map[string]*view.Resource, routers map[string]*router.Router, changed bool) (map[string]*router.Router, bool, error) {
	routersChan := make(chan func() (*router.Resource, string, error))
	channelSize := r.populateRoutersChan(ctx, routersChan, r.changeSession.routerChanges.routersIndex.updated.data, resources)
	counter := 0
	var errors []error
	if channelSize > 0 {
		for fn := range routersChan {
			routerResource, URL, err := fn()
			if err != nil {
				errors = append(errors, fmt.Errorf("invalid %v,%w ", URL, err))
			} else {
				aRouter, err := r.NewRouter(routerResource)
				if err != nil {
					errors = append(errors, fmt.Errorf("invalid %v,%w ", URL, err))
				} else {
					r.changeSession.routerChanges.AddRouter(URL, aRouter)
				}
			}

			counter++
			if counter >= channelSize {
				close(routersChan)
			}
		}
	}
	if err := r.combineErrors("routers", errors); err != nil {
		return nil, false, err
	}

	return routers, true, nil
}

func (r *Service) NewRouter(routerResource *router.Resource) (*router.Router, error) {
	return router.New(routerResource, router.ApiPrefix(r.Config.APIPrefix))
}

func (r *Service) getDataResources(ctx context.Context, fs afs.Service) (*routerConfig, error) {
	changes, err := r.detectResourceChanges(ctx, fs)
	if err != nil {
		return nil, err
	}

	if !changes.Changed() {
		return &routerConfig{
			resources: copyResourcesMap(r.dataResourcesIndex),
		}, nil
	}

	pluginsChanges, err := r.handlePluginsChanges(ctx, changes)
	if pluginsChanges != nil {
	outer:
		for URL, viewResource := range r.dataResourcesIndex {
			for _, definition := range viewResource.Types {
				pkg := pluginsChanges.Types.Package(definition.Package)
				if pkg == nil {
					continue
				}
				rType, _ := pkg.Lookup(definition.Name)
				if rType != nil {
					changes.OnChange(resource.Modified, URL)
					continue outer
				}
			}
		}
	}

	if err != nil {
		return nil, err
	}

	resources, err := r.reloadResources(ctx, fs, changes)
	if err != nil {
		return nil, err
	}

	interceptors, err := r.buildInterceptors(ctx, changes.routersIndex)

	return &routerConfig{
		changed:      true,
		resources:    resources,
		interceptors: interceptors,
	}, err
}

func (r *Service) reloadResources(ctx context.Context, fs afs.Service, changes *RouterChanges) (map[string]*view.Resource, error) {
	result := map[string]*view.Resource{}
	for resourceURL, dataResource := range r.dataResourcesIndex {
		if changes.resourcesIndex.Changed(dataResource.SourceURL) {
			continue
		}

		result[resourceURL] = r.dataResourcesIndex[resourceURL]
	}

	if len(changes.resourcesIndex.updated.data) == 0 { //add to avoid deadlock
		return result, nil
	}

	resourceChan := make(chan func() (*view.Resource, string, error), len(changes.resourcesIndex.updated.data))
	channelSize := r.populateResourceChan(ctx, resourceChan, fs, changes.resourcesIndex.updated.data)
	counter := 0
	var errors []error
	for fn := range resourceChan {
		dependency, URL, err := fn()
		if err != nil {
			errors = append(errors, err)
		} else {
			result[URL] = dependency
		}

		counter++
		if counter >= channelSize {
			close(resourceChan)
		}
	}

	if err := r.combineErrors("dependencies", errors); err != nil {
		return nil, err
	}

	return result, nil
}

func (r *Service) combineErrors(resourceType string, errors []error) error {
	if len(errors) == 0 {
		return nil
	}

	actualErr := fmt.Errorf("failed to load %v due to the: %w", resourceType, errors[0])
	for i := 1; i < len(errors); i++ {
		actualErr = fmt.Errorf("%w, %v", actualErr, errors[i].Error())
	}

	return actualErr
}

func copyResourcesMap(index map[string]*view.Resource) map[string]*view.Resource {
	result := map[string]*view.Resource{}

	for key := range index {
		result[key] = index[key]
	}

	return result
}

func deepCopyResources(index map[string]*view.Resource) (map[string]*view.Resource, error) {
	marshal, err := json.Marshal(index)
	if err != nil {
		return nil, err
	}

	result := map[string]*view.Resource{}
	err = json.Unmarshal(marshal, &result)
	return result, err
}

func (r *Service) populateResourceChan(ctx context.Context, resourceChan chan func() (*view.Resource, string, error), fs afs.Service, updatedResources []string) int {
	for _, resourceURL := range updatedResources {
		go func(URL string) {
			newResource, err := r.loadDependencyResource(URL, ctx, fs)
			resourceChan <- func() (*view.Resource, string, error) {
				return newResource, r.updateResourceKey(URL), err
			}
		}(resourceURL)
	}

	return len(updatedResources)
}

func (r *Service) loadDependencyResource(URL string, ctx context.Context, fs afs.Service) (*view.Resource, error) {
	dependency, ok := r.changeSession.Dependencies[URL]
	if ok {
		return dependency, nil
	}

	var err error
	dependency, err = view.LoadResourceFromURL(ctx, URL, fs)
	return dependency, err
}

func (r *Service) detectResourceChanges(ctx context.Context, fs afs.Service) (*RouterChanges, error) {
	changes := NewResourcesChange(r.routersIndex)
	errors := shared.NewErrors(0)

	err := r.dependencyTracker.Notify(ctx, fs, func(URL string, operation resource.Operation) {
		changes.OnChange(operation, URL)
	})

	if err != nil {
		errors.Append(fmt.Errorf("[ERROR] failed to dependency: %v", err))
	}

	plugErr := r.pluginTracker.Notify(ctx, fs, func(URL string, operation resource.Operation) {
		if path.Ext(URL) != ".pinf" {
			return
		}
		changes.OnChange(operation, URL)
	})

	err = r.assetsTracker.Notify(ctx, fs, func(URL string, operation resource.Operation) {
		if path.Ext(URL) != ".rt" {
			return
		}
		changes.OnChange(operation, URL)
	})

	if err != nil {
		errors.Append(fmt.Errorf("failed to load interceptors: %v", err))
	}

	r.changeSession.OnDependencyUpdated(changes.resourcesIndex.updated.data...)
	r.changeSession.OnFileChange(changes.resourcesIndex.deleted.data...)
	if plugErr != nil {
		errors.Append(fmt.Errorf("failed to load plugin:  %v", plugErr))
	}

	return changes, errors.Error()
}

func (r *Service) detectRoutersChanges(ctx context.Context, fs afs.Service) error {
	if err := r.routeTracker.Notify(ctx, fs, func(URL string, operation resource.Operation) {
		r.changeSession.routerChanges.OnChange(operation, URL)
	}); err != nil {
		return err
	}

	r.changeSession.routerChanges.AfterResourceChanges()

	return nil
}

func (r *Service) PreCachables(method string, uri string) ([]*view.View, error) {
	aRouter, ok := r.Router()
	if !ok {
		return []*view.View{}, nil
	}

	return aRouter.PreCacheables(method, uri)
}

func (r *Service) updateResourceKey(URL string) string {
	_, key := furl.Split(URL, file.Scheme)
	if index := strings.Index(key, "."); index != -1 {
		key = key[:index]
	}

	return key
}

func (r *Service) updateRouterAPIKeys(routes router.Routes) {
	for _, route := range routes {
		if route.APIKey == nil {
			route.APIKey = r.Config.APIKeys.Match(route.URI)
		}
	}
}

func (r *Service) populateRoutersChan(ctx context.Context, routersChan chan func() (*router.Resource, string, error), routersUpdated []string, resources map[string]*view.Resource) int {
	for _, resourceURL := range routersUpdated {
		go func(URL string) {
			routerResource, err := r.loadRouterResource(ctx, URL, resources)
			routersChan <- func() (*router.Resource, string, error) {
				return routerResource, URL, err
			}
		}(resourceURL)
	}

	return len(routersUpdated)
}

func (r *Service) loadRouterResource(ctx context.Context, URL string, resources map[string]*view.Resource) (*router.Resource, error) {
	routerResource, ok := r.changeSession.Routers[URL]
	if ok {
		return routerResource, nil
	}

	copyResources, err := deepCopyResources(resources)
	if err != nil {
		return nil, err
	}

	routerResource, err = router.LoadResource(ctx, r.fs, URL, r.Config.Discovery(), r.configRegistry, r.metrics, copyResources)
	if err != nil {
		return nil, err
	}

	routerResource.Resource.SetFs(r.fs)
	r.changeSession.AddRouter(URL, routerResource)
	if err = r.updateCacheConnectorRefIfNeeded(routerResource); err != nil {
		return nil, err
	}

	if r.Config.DisableCors {
		routerResource.Cors = nil
	}

	if r.Config.RevealMetric != nil {
		routerResource.RevealMetric = r.Config.RevealMetric
	}
	return routerResource, routerResource.Init(ctx)
}

func (r *Service) updateCacheConnectorRefIfNeeded(routerResource *router.Resource) error {
	if r.Config.CacheConnectorPrefix == "" {
		return nil
	}

	for _, aView := range routerResource.Resource.Views {
		if err := r.updateCacheConnectorRef(routerResource, aView); err != nil {
			return err
		}
	}

	for _, route := range routerResource.Routes {
		if err := r.updateCacheConnectorRef(routerResource, route.View); err != nil {
			return err
		}
	}

	return nil
}

func (r *Service) updateCacheConnectorRef(routerResource *router.Resource, aView *view.View) error {
	viewWarmup, ok := r.viewWarmup(aView)
	if ok {
		if viewWarmup.Connector != nil && viewWarmup.Connector.Ref != "" {
			cacheConnectorName := r.Config.CacheConnectorPrefix + viewWarmup.Connector.Ref
			if routerResource.Resource.ExistsConnector(cacheConnectorName) {
				viewWarmup.Connector.Ref = cacheConnectorName
			}
		} else if viewWarmup.Connector == nil {
			viewConnector, ok := r.viewConnector(routerResource, aView)
			if ok {
				refName := r.Config.CacheConnectorPrefix + viewConnector.Name
				if ok && routerResource.Resource.ExistsConnector(refName) {
					viewWarmup.Connector = &view.Connector{Reference: shared.Reference{Ref: refName}}
				}
			}
		}
	}

	for _, relation := range aView.With {
		if err := r.updateCacheConnectorRef(routerResource, &relation.Of.View); err != nil {
			return err
		}
	}

	return nil
}

func (r *Service) viewWarmup(aView *view.View) (*view.Warmup, bool) {
	if aView.Cache == nil {
		return nil, false
	}

	return aView.Cache.Warmup, aView.Cache.Warmup != nil
}

func (r *Service) viewConnector(routerResource *router.Resource, aView *view.View) (*view.Connector, bool) {
	if aView.Connector.Name != "" {
		return aView.Connector, true
	}

	if aView.Connector.Ref != "" {
		connector, err := routerResource.Resource.Connector(aView.Connector.Ref)
		return connector, err == nil
	}

	return nil, false
}

func initSecrets(ctx context.Context, config *Config) error {
	if len(config.Secrets) == 0 {
		return nil
	}
	secrets := secret.New()
	for _, sec := range config.Secrets {
		if err := secrets.Apply(ctx, sec); err != nil {
			return err
		}
	}

	return nil
}

func (r *Service) WrapResponseIfNeeded(response http.ResponseWriter) http.ResponseWriter {
	if !r.ShouldRevealMetrics() {
		return response
	}

	return router.NewMetricResponse(response)
}

func (r *Service) ShouldRevealMetrics() bool {
	return r.Config.RevealMetric != nil && *r.Config.RevealMetric
}

func (r *Service) LogInitTimeIfNeeded(start time.Time, writer http.ResponseWriter) {
	if !r.ShouldRevealMetrics() {
		return
	}

	writer.Header().Set(httputils.DatlyServiceInitHeader, time.Since(start).String())
}

func (r *Service) buildInterceptors(ctx context.Context, index *ExtIndex) (router.RouterInterceptors, error) {
	interceptors := r.interceptors.Copy()
	for _, key := range index.deleted.data {
		delete(interceptors, key)
	}

	expectedSize := len(index.updated.data)
	if expectedSize == 0 {
		return interceptors, nil
	}

	resultChan := make(chan func() (*router.RouteInterceptor, error), expectedSize)
	for _, URL := range index.updated.data {
		go func(ctx context.Context, URL string, collector chan func() (*router.RouteInterceptor, error)) {
			resultChan <- func() (*router.RouteInterceptor, error) {
				return router.NewInterceptorFromURL(ctx, r.fs, URL, r.configRegistry.Types.Lookup)
			}
		}(ctx, URL, resultChan)
	}

	var i = 0
	var resultErr error

	for fn := range resultChan {
		i++
		if i == expectedSize {
			close(resultChan)
		}

		interceptor, err := fn()
		if err != nil {
			resultErr = err
			continue
		}

		interceptors[interceptor.SourceURL] = interceptor
	}

	return interceptors, resultErr
}

func (r *Service) ServeHTTPAsync(writer http.ResponseWriter, request *http.Request, record *async2.Job) {
	//aRouter, writer, ok := r.router(writer)
	//if !ok {
	//	return
	//}

	//	aRouter.HandleAsync(writer, request, record)
}

func (r *Service) routerProvider() NewRouterFn {
	return func(ctx context.Context, URI string) (*router.Router, error) {
		routerResource, err := r.loadRouterResource(ctx, URI, r.dataResourcesIndex)
		if err != nil {
			return nil, err
		}

		return r.NewRouter(routerResource)
	}
}
