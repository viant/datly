package gateway

import (
	"context"
	"fmt"
	"github.com/viant/afs"
	"github.com/viant/afs/cache"
	"github.com/viant/afs/file"
	furl "github.com/viant/afs/url"
	"github.com/viant/cloudless/resource"
	"github.com/viant/datly/auth/secret"
	"github.com/viant/datly/codec"
	"github.com/viant/datly/router"
	"github.com/viant/datly/view"
	"github.com/viant/gmetric"
	"net/http"
	"strings"
	"sync"
	"time"
)

type Service struct {
	Config               *Config
	visitors             codec.Visitors
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
}

func (r *Service) ServeHTTP(writer http.ResponseWriter, request *http.Request) {
	aRouter, ok := r.Router()
	if !ok {
		writer.WriteHeader(http.StatusNotFound)
	}

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
func New(ctx context.Context, config *Config, statusHandler http.Handler, authorizer Authorizer, visitors codec.Visitors, types view.Types, metrics *gmetric.Service) (*Service, error) {
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
	}

	if err = initSecrets(ctx, config); err != nil {
		return nil, err
	}

	err = srv.createRouterIfNeeded(ctx, metrics, statusHandler, authorizer)
	srv.detectChanges(metrics, statusHandler, authorizer)

	return srv, err
}

func (r *Service) createRouterIfNeeded(ctx context.Context, metrics *gmetric.Service, statusHandler http.Handler, authorizer Authorizer) error {
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
	r.mux.Unlock()

	return nil
}

func (r *Service) getRouters(ctx context.Context, fs afs.Service, resources map[string]*view.Resource, viewResourcesChanged bool) (routers map[string]*router.Router, changed bool, err error) {
	updatedRouters, removedRouters, err := r.detectRoutersChanges(ctx, fs)
	if err != nil {
		return nil, false, err
	}

	if !viewResourcesChanged && len(updatedRouters) == 0 && len(removedRouters) == 0 {
		return r.routersIndex, false, nil
	}

	updatedMap, removedMap := asMap(updatedRouters), asMap(removedRouters)

	routers = map[string]*router.Router{}
	for routerURL := range r.routersIndex {
		if (updatedMap[routerURL] || removedMap[routerURL]) && !changed {
			continue
		}

		routers[routerURL] = r.routersIndex[routerURL]
	}

	for _, resourceURL := range updatedRouters {
		routerResource, err := router.NewResourceFromURL(ctx, fs, resourceURL, r.Config.Discovery(), r.visitors, r.types, r.metrics, resources)
		if err != nil {
			return nil, false, err
		}

		r.updateRouterAPIKeys(routerResource.Routes)
		routerResource.SourceURL = resourceURL
		routers[resourceURL] = router.New(routerResource, router.ApiPrefix(r.Config.APIPrefix))
	}

	return routers, true, nil
}

func (r *Service) getDataResources(ctx context.Context, fs afs.Service) (resources map[string]*view.Resource, changed bool, err error) {
	updatedResources, removedResources, err := r.detectResourceChanges(ctx, fs)
	if err != nil {
		return nil, false, err
	}

	if len(updatedResources) == 0 && len(removedResources) == 0 {
		return r.dataResourcesIndex, false, nil
	}

	updatedMap, removedMap := asMap(updatedResources), asMap(removedResources)

	result := map[string]*view.Resource{}
	for resourceURL, dataResource := range r.dataResourcesIndex {
		if updatedMap[dataResource.SourceURL] || removedMap[dataResource.SourceURL] {
			continue
		}

		result[resourceURL] = r.dataResourcesIndex[resourceURL]
	}

	for _, resourceURL := range updatedResources {
		newResource, err := view.LoadResourceFromURL(ctx, resourceURL, fs)
		if err != nil {
			return nil, false, err
		}

		result[r.updateResourceKey(resourceURL)] = newResource
	}

	return result, true, err
}

func asMap(URLs []string) map[string]bool {
	result := map[string]bool{}
	for i := range URLs {
		result[URLs[i]] = true
	}

	return result
}

func (r *Service) detectResourceChanges(ctx context.Context, fs afs.Service) ([]string, []string, error) {
	var updatedResources []string
	var removedResources []string

	err := r.dataResourceTracker.Notify(ctx, fs, func(URL string, operation resource.Operation) {
		if strings.Contains(URL, ".meta/") {
			return
		}

		switch operation {
		case resource.Added, resource.Modified:
			updatedResources = append(updatedResources, URL)
		case resource.Deleted:
			removedResources = append(removedResources, URL)
		}
	})

	return updatedResources, removedResources, err
}

func (r *Service) detectRoutersChanges(ctx context.Context, fs afs.Service) ([]string, []string, error) {
	var updated []string
	var removed []string
	err := r.routeResourceTracker.Notify(ctx, fs, func(URL string, operation resource.Operation) {
		if strings.Contains(URL, ".meta/") || !strings.HasSuffix(URL, ".yaml") {
			return
		}

		switch operation {
		case resource.Added, resource.Modified:
			updated = append(updated, URL)
		case resource.Deleted:
			removed = append(removed, URL)
		}
	})

	return updated, removed, err
}

func (r *Service) detectChanges(metrics *gmetric.Service, statusHandler http.Handler, authorizer Authorizer) {
	ctx := context.Background()
	cancel, cancelFunc := context.WithCancel(ctx)
	r.cancelFn = cancelFunc
	go func() {
	outer:
		for {
			time.Sleep(time.Minute * 1)
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
