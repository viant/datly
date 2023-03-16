package gateway

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/viant/afs"
	"github.com/viant/afs/cache"
	"github.com/viant/afs/file"
	"github.com/viant/afs/matcher"
	furl "github.com/viant/afs/url"
	"github.com/viant/cloudless/resource"
	"github.com/viant/datly/auth/secret"
	"github.com/viant/datly/config"
	"github.com/viant/datly/router"
	"github.com/viant/datly/shared"
	"github.com/viant/datly/view"
	"github.com/viant/gmetric"
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
		Config                *Config
		routersIndex          map[string]*router.Router
		fs                    afs.Service
		routeResourceTracker  *resource.Tracker
		dataResourceTracker   *resource.Tracker
		dataResourcesIndex    map[string]*view.Resource
		metrics               *gmetric.Service
		mainRouter            *Router
		cancelFn              context.CancelFunc
		changeSession         *Session
		JWTSigner             *signer.Service
		pluginsInUse          map[string]bool
		mux                   sync.RWMutex
		pluginsConfig         *config.Registry
		pluginResourceTracker *resource.Tracker
		statusHandler         http.Handler
		authorizer            Authorizer
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

//New creates gateway Service. It is important to call Service.Close before Service got Garbage collected.
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

	srv := &Service{
		metrics:               metrics,
		pluginsConfig:         registry,
		Config:                aConfig,
		mux:                   sync.RWMutex{},
		fs:                    fs,
		dataResourcesIndex:    map[string]*view.Resource{},
		routeResourceTracker:  resource.New(aConfig.RouteURL, time.Duration(aConfig.SyncFrequencyMs)*time.Millisecond),
		dataResourceTracker:   resource.New(aConfig.DependencyURL, time.Duration(aConfig.SyncFrequencyMs)*time.Millisecond),
		pluginResourceTracker: resource.New(aConfig.PluginsURL, time.Duration(aConfig.SyncFrequencyMs)*time.Millisecond),
		routersIndex:          map[string]*router.Router{},
		mainRouter:            NewRouter(map[string]*router.Router{}, aConfig, metrics, statusHandler, authorizer),
		changeSession:         NewSession(aConfig.ChangeDetection),
		pluginsInUse:          map[string]bool{},
		statusHandler:         statusHandler,
		authorizer:            authorizer,
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
	//srv.detectChanges(metrics, statusHandler, authorizer)
	fmt.Printf("initialised datly: %s\n", time.Now().Sub(start))
	return srv, err
}

func newFileService(aConfig *Config) (afs.Service, error) {
	if !aConfig.UseCacheFS {
		return afs.New(), nil
	}

	URL, err := CommonURL(aConfig.DependencyURL, aConfig.PluginsURL, aConfig.RouteURL)
	if err != nil {
		return nil, err
	}

	return cache.Singleton(URL, &matcher.Ignore{Ext: map[string]bool{
		".so": true,
		"so":  true,
	}}), nil
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
				return "", fmt.Errorf("paths don't match, wanted %v got %v", base, dir)
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
	defer func() {
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
	resources, changed, err := r.getDataResources(ctx, r.fs)
	if err != nil {
		return err
	}

	routers, changed, err := r.getRouters(ctx, r.fs, resources, changed, isFirst)
	if err != nil || !changed {
		return err
	}

	if !isFirst {
		fmt.Printf("[INFO] routers rebuild completed\n")
	}

	mainRouter := NewRouter(routers, r.Config, metrics, statusHandler, authorizer)
	r.mux.Lock()
	r.mainRouter = mainRouter
	r.routersIndex = routers
	r.dataResourcesIndex = resources
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
		fmt.Printf("[INFO] detected resources changes, rebuilding routers\n")
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

	pluginsChanges, err := r.handlePluginsChanges(ctx, changes)
	if pluginsChanges != nil {
	outer:
		for URL, viewResource := range r.dataResourcesIndex {
			for _, definition := range viewResource.Types {
				_, ok := pluginsChanges.PackageRegistry(definition.Package)[definition.Name]
				if ok {
					changes.OnChange(resource.Modified, URL)
					continue outer
				}
			}
		}
	}

	if err != nil {
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
	fmt.Printf("[INFO] Dependencies check ...\n")
	depErr := r.dataResourceTracker.Notify(ctx, fs, func(URL string, operation resource.Operation) {
		for _, folderName := range unindexedFolders {
			fmt.Printf("[INFO] Dependency changes: %v, Operation: %v\n", URL, operation)

			if strings.Contains(URL, folderName) {
				return
			}
		}
		changes.OnChange(operation, URL)
	})

	fmt.Printf("[INFO] Plugin check ...\n")
	plugErr := r.pluginResourceTracker.Notify(ctx, fs, func(URL string, operation resource.Operation) {
		fmt.Printf("[INFO] Plugin change: %v, Operation: %v\n", URL, operation)

		for _, folderName := range unindexedFolders {
			if strings.Contains(URL, folderName) {
				return
			}
		}

		URL = strings.Replace(URL, ".info", ".so", 1)
		if ok, err := fs.Exists(ctx, URL); !ok || err != nil {
			return
		}

		changes.OnChange(operation, URL)
	})
	r.changeSession.OnDependencyUpdated(changes.resourcesIndex.updated...)
	r.changeSession.OnFileChange(changes.resourcesIndex.deleted...)
	var err error
	if depErr != nil {
		err = depErr
	}
	if plugErr != nil {
		err = fmt.Errorf("failed to load pluging: %w, %v", depErr, err)
	}
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

//
//func (r *Service) detectChanges(metrics *gmetric.Service, statusHandler http.Handler, authorizer Authorizer) {
//	ctx := context.Background()
//	cancel, cancelFunc := context.WithCancel(ctx)
//	r.cancelFn = cancelFunc
//	go func() {
//	outer:
//		for {
//			time.Sleep(r.Config.ChangeDetection._retry)
//			select {
//			case <-cancel.Done():
//				break outer
//			default:
//				fmt.Printf("[INFO] Waking up to detect changes ...\n")
//				if err := r.syncChangesIfNeeded(context.TODO(), metrics, statusHandler, authorizer, false); err != nil {
//					fmt.Printf("[ERROR] error occured while recreating routers: %v \n", err.Error())
//				}
//			}
//		}
//	}()
//}

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

	routerResource, err = router.LoadResource(ctx, fs, URL, r.Config.Discovery(), r.pluginsConfig, r.metrics, copyResources)
	if err != nil {
		return nil, err
	}
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

	writer.Header().Set(router.DatlyServiceInitHeader, time.Since(start).String())
}
