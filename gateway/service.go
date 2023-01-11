package gateway

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/viant/afs"
	"github.com/viant/afs/cache"
	"github.com/viant/afs/file"
	furl "github.com/viant/afs/url"
	"github.com/viant/cloudless/resource"
	"github.com/viant/datly/auth/secret"
	"github.com/viant/datly/router"
	"github.com/viant/datly/shared"
	"github.com/viant/datly/view"
	"github.com/viant/datly/xdatly"
	"github.com/viant/gmetric"
	"github.com/viant/scy/auth/jwt/signer"
	"net/http"
	"path"
	"plugin"
	"strings"
	"sync"
	"time"
)

type (
	Service struct {
		Config               *Config
		visitors             xdatly.CodecsRegistry
		types                view.Types
		mux                  sync.RWMutex
		routersIndex         map[string]*router.Router
		fs                   afs.Service
		cfs                  afs.Service //cache file system
		routeResourceTracker *resource.Tracker
		dataResourceTracker  *resource.Tracker
		dataResourcesIndex   map[string]*view.Resource
		metrics              *gmetric.Service
		mainRouter           *Router
		cancelFn             context.CancelFunc
		session              *Session
		JWTSigner            *signer.Service
	}
)

func (r *Service) ServeHTTP(writer http.ResponseWriter, request *http.Request) {
	aRouter, ok := r.Router()
	if !ok {
		writer.WriteHeader(http.StatusNotFound)
	}

	writer = r.WrapResponseIfNeeded(writer)
	aRouter.Handle(writer, request)
}

func (r *Service) Router() (*Router, bool) {
	mainRouter := r.mainRouter
	return mainRouter, mainRouter != nil
}

func (r *Service) Close() error {
	if r.cancelFn != nil {
		r.cancelFn()
	}

	return nil
}

//New creates gateway Service. It is important to call Service.Close before Service got Garbage collected.
func New(ctx context.Context, config *Config, statusHandler http.Handler, authorizer Authorizer, visitors xdatly.CodecsRegistry, types view.Types, metrics *gmetric.Service) (*Service, error) {
	start := time.Now()
	config.Init()
	err := config.Validate()
	if err != nil {
		return nil, err
	}

	URL, _ := furl.Split(config.RouteURL, file.Scheme)
	cfs := cache.Singleton(URL)

	srv := &Service{
		visitors:             visitors,
		metrics:              metrics,
		types:                types,
		Config:               config,
		mux:                  sync.RWMutex{},
		fs:                   afs.New(),
		cfs:                  cfs,
		dataResourcesIndex:   map[string]*view.Resource{},
		routeResourceTracker: resource.New(config.RouteURL, time.Duration(config.SyncFrequencyMs)*time.Millisecond),
		dataResourceTracker:  resource.New(config.DependencyURL, time.Duration(config.SyncFrequencyMs)*time.Millisecond),
		routersIndex:         map[string]*router.Router{},
		mainRouter:           NewRouter(map[string]*router.Router{}, config, metrics, statusHandler, authorizer),
		session:              NewSession(config.ChangeDetection),
	}

	if config.JwtSigner != nil {
		srv.JWTSigner = signer.New(config.JwtSigner)
		if err = srv.JWTSigner.Init(context.Background()); err != nil {
			return nil, err
		}
	}

	if err = initSecrets(ctx, config); err != nil {
		return nil, err
	}

	err = srv.createRouterIfNeeded(ctx, metrics, statusHandler, authorizer)
	srv.detectChanges(metrics, statusHandler, authorizer)
	fmt.Printf("initialised datly: %s\n", time.Now().Sub(start))
	return srv, err
}

func (r *Service) createRouterIfNeeded(ctx context.Context, metrics *gmetric.Service, statusHandler http.Handler, authorizer Authorizer) error {
	defer func() {
		if r.session == nil {
			return
		}

		r.session.UpdateFailureCounter()
	}()

	if r.session == nil {
		r.session = NewSession(r.Config.ChangeDetection)
	}

	fs := r.reloadFs()
	resources, changed, err := r.getDataResources(ctx, fs)
	if err != nil {
		return err
	}

	routers, changed, err := r.getRouters(ctx, fs, resources, changed)
	if err != nil || !changed {
		return err
	}

	mainRouter := NewRouter(routers, r.Config, metrics, statusHandler, authorizer)
	r.mux.Lock()
	r.mainRouter = mainRouter
	r.routersIndex = routers
	r.dataResourcesIndex = resources
	r.session = nil
	r.mux.Unlock()

	return nil
}

func (r *Service) getRouters(ctx context.Context, fs afs.Service, resources map[string]*view.Resource, viewResourcesChanged bool) (routers map[string]*router.Router, changed bool, err error) {
	updatedMap, removedMap, err := r.detectRoutersChanges(ctx, fs)
	if err != nil {
		return nil, false, err
	}

	if !viewResourcesChanged && len(updatedMap) == 0 && len(removedMap) == 0 {
		return r.routersIndex, false, nil
	}

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
	for fn := range routersChan {
		routerResource, URL, err := fn()
		if err != nil {
			errors = append(errors, err)
		} else {
			routers[URL] = router.New(routerResource, router.ApiPrefix(r.Config.APIPrefix))
		}

		counter++
		if counter >= channelSize {
			close(routersChan)
		}
	}

	if err := r.combineErrors("routers", errors); err != nil {
		return nil, false, err
	}

	return routers, true, nil
}

func (r *Service) getDataResources(ctx context.Context, fs afs.Service) (resources map[string]*view.Resource, changed bool, err error) {
	changes, err := r.detectResourceChanges(ctx, fs)
	if err != nil {
		return nil, false, err
	}

	if !changes.Changed() {
		return copyResourcesMap(r.dataResourcesIndex), false, nil
	}

	if err = r.handlePluginsChanges(changes); err != nil {
		return nil, false, err
	}

	resources, err = r.reloadResources(ctx, fs, changes)
	if err != nil {
		return nil, false, err
	}

	return resources, true, nil
}

func (r *Service) reloadResources(ctx context.Context, fs afs.Service, changes *ResourcesChange) (map[string]*view.Resource, error) {
	result := map[string]*view.Resource{}
	for resourceURL, dataResource := range r.dataResourcesIndex {
		if changes.resourcesIndex.Changed(dataResource.SourceURL) {
			continue
		}

		result[resourceURL] = r.dataResourcesIndex[resourceURL]
	}

	resourceChan := make(chan func() (*view.Resource, string, error))
	channelSize := r.populateResourceChan(ctx, resourceChan, fs, changes.resourcesIndex.updatedIndex)
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
	return result, json.Unmarshal(marshal, &result)
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
	dependency, ok := r.session.Dependencies[URL]
	if ok {
		return dependency, nil
	}

	var err error
	dependency, err = view.LoadResourceFromURL(ctx, URL, fs)
	return dependency, err
}

func (r *Service) detectResourceChanges(ctx context.Context, fs afs.Service) (*ResourcesChange, error) {
	changes := NewResourcesChange()
	err := r.dataResourceTracker.Notify(ctx, fs, func(URL string, operation resource.Operation) {
		if strings.Contains(URL, ".meta/") {
			return
		}

		changes.OnChange(operation, URL)
	})

	if err != nil {
		return nil, err
	}

	r.session.OnDependencyUpdated(changes.resourcesIndex.updated...)
	r.session.OnFileChange(changes.resourcesIndex.deleted...)
	return changes, err
}

func (r *Service) detectRoutersChanges(ctx context.Context, fs afs.Service) (map[string]bool, map[string]bool, error) {
	var updated []string
	var deleted []string
	var metaUpdated []string
	var updatedSQLs []string
	err := r.routeResourceTracker.Notify(ctx, fs, func(URL string, operation resource.Operation) {
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

	r.session.OnRouterUpdated(updated...)
	r.session.OnRouterDeleted(deleted...)

	routerMetaUpdated := r.handleMetaUpdated(metaUpdated)
	r.session.OnRouterUpdated(routerMetaUpdated...)

	return r.session.UpdatedRouters, r.session.DeletedRouters, err
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

func (r *Service) detectChanges(metrics *gmetric.Service, statusHandler http.Handler, authorizer Authorizer) {
	ctx := context.Background()
	cancel, cancelFunc := context.WithCancel(ctx)
	r.cancelFn = cancelFunc
	go func() {
	outer:
		for {
			time.Sleep(r.Config.ChangeDetection._retry)
			select {
			case <-cancel.Done():
				break outer
			default:
				if err := r.createRouterIfNeeded(context.TODO(), metrics, statusHandler, authorizer); err != nil {
					fmt.Printf("error occured while recreating routers: %v \n", err.Error())
				}
			}
		}
	}()
}

func (r *Service) reloadFs() afs.Service {
	if r.Config.UseCacheFS {
		return r.cfs
	}
	return r.fs
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
	routerResource, ok := r.session.Routers[URL]
	if ok {
		return routerResource, nil
	}

	copyResources, err := deepCopyResources(resources)
	if err != nil {
		return nil, err
	}

	routerResource, err = router.LoadResource(ctx, fs, URL, r.Config.Discovery(), r.visitors, r.types, r.metrics, copyResources)
	if err != nil {
		return nil, err
	}
	r.session.AddRouter(URL, routerResource)
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

		if r.session.DeletedRouters[routeURL] || r.session.UpdatedRouters[routeURL] {
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

	writer.Header().Set(router.DatlyServiceInitHeader, time.Since(start).String())
}

func (r *Service) handlePluginsChanges(changes *ResourcesChange) error {
	updateSize := len(changes.pluginsIndex.updated)
	switch updateSize {
	case 0:
		return nil
	case 1:
		return r.loadPlugin(changes.pluginsIndex.updated[0])
	default:
		errorsChan := make(chan error, updateSize)
		for _, pluginURL := range changes.pluginsIndex.updated {
			go func(collector chan error, URL string) {
				errorsChan <- r.loadPlugin(URL)
			}(errorsChan, pluginURL)
		}

		counter := 0
		for err := range errorsChan {
			counter++
			if err != nil {
				return err
			}

			if counter == updateSize {
				return nil
			}
		}
	}

	return nil
}

func (r *Service) loadPlugin(URL string) error {
	if index := strings.Index(URL, r.Config.DependencyURL); index != -1 {
		URL = URL[index:]
	}

	plugins, err := plugin.Open(URL)
	if err != nil {
		return err
	}

	configPlugin, err := plugins.Lookup(xdatly.PluginSymbol)
	if err != nil {
		return err
	}

	registry, ok := configPlugin.(**xdatly.Registry)
	if !ok {
		return fmt.Errorf("unexpected symbol type, wanted %T got %T", &xdatly.Registry{}, configPlugin)
	}

	xdatly.Config.Override(*registry)
	return nil
}
