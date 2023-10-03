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
	"github.com/viant/datly/repository/extension"
	"github.com/viant/datly/service/auth/secret"
	"github.com/viant/datly/utils/httputils"
	pbuild "github.com/viant/pgo/build"
	"sync/atomic"

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

		dataResourcesIndex map[string]*view.Resource
		metrics            *gmetric.Service
		mainRouter         *Router
		cancelFn           context.CancelFunc
		changeSession      *Session
		JWTSigner          *signer.Service
		mux                sync.RWMutex
		configRegistry     *extension.Registry
		pluginManager      *manager.Service
		statusHandler      http.Handler
		authorizer         Authorizer

		nextCheck  time.Time
		isBuilding int64
	}

	routerConfig struct {
		changed   bool
		resources map[string]*view.Resource
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
func New(ctx context.Context, aConfig *Config, statusHandler http.Handler, authorizer Authorizer, registry *extension.Registry, metrics *gmetric.Service) (*Service, error) {
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
	syncFrequency := time.Duration(aConfig.SyncFrequencyMs) * time.Millisecond
	mainRouter, err := NewRouter(map[string]*router.Router{}, aConfig, metrics, statusHandler, authorizer)
	if err != nil {
		return nil, err
	}

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
			syncFrequency,
		),
		dependencyTracker: NewNotifier(
			aConfig.DependencyURL,
			fs,
			syncFrequency,
		),
		pluginTracker: NewNotifier(
			aConfig.PluginsURL,
			fs,
			syncFrequency,
		),
		routersIndex:  map[string]*router.Router{},
		mainRouter:    mainRouter,
		changeSession: NewSession(aConfig.ChangeDetection),
		statusHandler: statusHandler,
		authorizer:    authorizer,
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
		r.changeSession = NewSession(r.Config.ChangeDetection)
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

	mainRouter, err := NewRouter(routers, r.Config, metrics, statusHandler, authorizer)
	if err != nil {
		return err
	}

	r.mux.Lock()
	r.mainRouter = mainRouter
	r.routersIndex = routers
	r.dataResourcesIndex = aRouterConfig.resources
	r.changeSession = nil
	r.mux.Unlock()

	return nil
}

func (r *Service) getRouters(ctx context.Context, fs afs.Service, resources map[string]*view.Resource, viewResourcesChanged bool, isFirst bool) (routers map[string]*router.Router, changed bool, err error) {
	updatedMap, removedMap, err := r.detectRoutersChanges(ctx, fs)
	if err != nil {
		return nil, false, err
	}

	if !viewResourcesChanged && len(updatedMap) == 0 && len(removedMap) == 0 {
		return r.routersIndex, false, nil
	}

	if !isFirst {
		fmt.Printf("[INFO] detected resourceLoader changes, rebuilding routers\n")
	}

	return r.rebuildRouters(ctx, fs, resources, routers, updatedMap, removedMap, changed)
}

func (r *Service) rebuildRouters(ctx context.Context, fs afs.Service, resources map[string]*view.Resource, routers map[string]*router.Router, updatedMap map[string]bool, removedMap map[string]bool, changed bool) (map[string]*router.Router, bool, error) {
	routers = map[string]*router.Router{}
	for routerURL := range r.routersIndex {
		if (updatedMap[routerURL] || removedMap[routerURL]) && !changed {
			continue
		}

		routers[routerURL] = r.routersIndex[routerURL]
	}

	routersChan := make(chan func() (*router.Resource, string, error))
	channelSize := r.populateRoutersChan(ctx, routersChan, updatedMap, fs, resources)
	counter := 0
	var errors []error
	if channelSize > 0 {
		for fn := range routersChan {
			routerResource, URL, err := fn()
			if err != nil {
				errors = append(errors, fmt.Errorf("invalid %v,%w ", URL, err))
			} else {
				routers[URL], err = router.New(routerResource, router.ApiPrefix(r.Config.APIPrefix))
				if err != nil {
					errors = append(errors, fmt.Errorf("invalid %v,%w ", URL, err))
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
	return &routerConfig{
		changed:   true,
		resources: resources,
	}, err
}

func (r *Service) reloadResources(ctx context.Context, fs afs.Service, changes *ResourcesChange) (view.NamedResources, error) {
	resources := resourceLoader{}
	for resourceURL, dataResource := range r.dataResourcesIndex {
		if !changes.resourcesIndex.Changed(dataResource.SourceURL) {
			continue
		}
		resources.WaitGroup.Add(1)
		go resources.load(ctx, fs, resourceURL)
	}

	for resourceURL := range changes.resourcesIndex.updatedIndex {
		resources.WaitGroup.Add(1)
		go resources.load(ctx, fs, resourceURL)
	}

	resources.WaitGroup.Wait()
	if err := r.combineErrors("dependencies", resources.errors); err != nil {
		return nil, err
	}
	return resources.byName, nil
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

func (r *Service) populateResourceChan(ctx context.Context, resourceChan chan func() (*view.Resource, string, error), fs afs.Service, updatedResources map[string]bool) int {
	for resourceURL := range updatedResources {
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

func (r *Service) detectResourceChanges(ctx context.Context, fs afs.Service) (*ResourcesChange, error) {
	changes := NewResourcesChange()
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

	if err != nil {
		errors.Append(fmt.Errorf("failed to load interceptors: %v", err))
	}

	r.changeSession.OnDependencyUpdated(changes.resourcesIndex.updated...)
	r.changeSession.OnFileChange(changes.resourcesIndex.deleted...)
	if plugErr != nil {
		errors.Append(fmt.Errorf("failed to load plugin:  %v", plugErr))
	}

	return changes, errors.Error()
}

func (r *Service) detectRoutersChanges(ctx context.Context, fs afs.Service) (map[string]bool, map[string]bool, error) {
	var updated []string
	var deleted []string
	var metaUpdated []string
	var updatedSQLs []string
	err := r.routeTracker.Notify(ctx, fs, func(URL string, operation resource.Operation) {
		if strings.Contains(URL, ".meta/") {
			metaUpdated = append(metaUpdated, URL[strings.LastIndexByte(URL, '/')+1:])
			return
		}
		if strings.HasSuffix(URL, ".sql") {
			updatedSQLs = append(updatedSQLs, URL)
			return
		}
		if !strings.HasSuffix(URL, ".yaml") {
			return
		}
		switch operation {
		case resource.Added, resource.Modified:
			updated = append(updated, URL)
		case resource.Deleted:
			deleted = append(deleted, URL)
		}
	})

	if err != nil {
		return nil, nil, err
	}

	for routerURL, aRouter := range r.routersIndex {
		if r.routerSQLChanged(aRouter, updatedSQLs) {
			updated = append(updated, routerURL)
		}
	}

	r.changeSession.OnRouterUpdated(updated...)
	r.changeSession.OnRouterDeleted(deleted...)

	routerMetaUpdated := r.handleMetaUpdated(metaUpdated)
	r.changeSession.OnRouterUpdated(routerMetaUpdated...)

	return r.changeSession.UpdatedRouters, r.changeSession.DeletedRouters, err
}

func (r *Service) handleMetaUpdated(metaUpdated []string) []string {
	if len(metaUpdated) == 0 {
		return nil
	}

	routeURLs := make([]string, 0, len(r.routersIndex))
	for URL := range r.routersIndex {
		routeURLs = append(routeURLs, URL)
	}

	var actualUpdated []string
	for _, viewSeg := range metaUpdated {
		if URL, ok := r.shouldUpdateRouter(viewSeg, routeURLs); ok {
			fmt.Printf("[INFO] Detected meta file missing. In order to optimize startup, please provide the %v cache file \n", path.Join(".meta", viewSeg))
			actualUpdated = append(actualUpdated, URL)
		}
	}

	return actualUpdated
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

func (r *Service) populateRoutersChan(ctx context.Context, routersChan chan func() (*router.Resource, string, error), updatedMap map[string]bool, fs afs.Service, resources map[string]*view.Resource) int {
	for resourceURL := range updatedMap {
		go func(URL string) {
			routerResource, err := r.loadRouterResource(URL, resources, ctx, fs)
			routersChan <- func() (*router.Resource, string, error) {
				return routerResource, URL, err
			}
		}(resourceURL)
	}

	return len(updatedMap)
}

func (r *Service) loadRouterResource(URL string, resources map[string]*view.Resource, ctx context.Context, fs afs.Service) (*router.Resource, error) {
	routerResource, ok := r.changeSession.Routers[URL]
	if ok {
		return routerResource, nil
	}

	copyResources, err := deepCopyResources(resources)
	if err != nil {
		return nil, err
	}

	routerResource, err = router.LoadResource(ctx, fs, URL, r.Config.Discovery(), r.configRegistry, r.metrics, copyResources)
	if err != nil {
		return nil, err
	}
	routerResource.Resource.SetFs(fs)
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

func (r *Service) shouldUpdateRouter(viewSeg string, routeURLs []string) (string, bool) {
	for _, routeURL := range routeURLs {
		if !strings.Contains(routeURL, r.Config.RouteURL) && strings.HasSuffix(routeURL, viewSeg) {
			continue
		}

		if r.changeSession.DeletedRouters[routeURL] || r.changeSession.UpdatedRouters[routeURL] {
			continue
		}

		return routeURL, true
	}

	return "", false
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

func (r *Service) routerSQLChanged(aRouter *router.Router, sqls []string) bool {
	if len(sqls) == 0 {
		return false
	}

	routes := aRouter.Routes("")
	for _, route := range routes {
		if r.viewSQLChanged(route.View, sqls) {
			return true
		}
	}

	return false
}

func (r *Service) viewSQLChanged(aView *view.View, sqlFiles []string) bool {
	if len(sqlFiles) == 0 {
		return false
	}

	if aView.Template.SourceURL != "" {
		for _, sqlFile := range sqlFiles {
			if strings.HasSuffix(sqlFile, aView.Template.SourceURL) {
				return true
			}
		}
	}

	for _, relation := range aView.With {
		if r.viewSQLChanged(&relation.Of.View, sqlFiles) {
			return true
		}
	}

	return false
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
