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
	"github.com/viant/datly/router"
	"github.com/viant/datly/view"
	"github.com/viant/datly/visitor"
	"github.com/viant/gmetric"
	"log"
	"net/http"
	"net/url"
	"os"
	"path"
	"strings"
	"sync"
	"time"
)

//Service represents gateway service
type Service struct {
	Config               *Config
	visitors             visitor.Visitors
	types                view.Types
	mux                  sync.RWMutex
	routerResources      []*router.Resource
	routers              map[string]*router.Router
	fs                   afs.Service
	cfs                  afs.Service //cache file system
	routeResourceTracker *resource.Tracker
	dataResourceTracker  *resource.Tracker
	dataResources        map[string]*view.Resource
	metrics              *gmetric.Service
}

func (r *Service) View(location string) (*view.View, error) {
	URI := strings.ReplaceAll(location, ".", "/")
	router, err := r.match(URI)
	if err != nil {
		return nil, err
	}
	name := URI
	if index := strings.LastIndex(name, "/"); index != -1 {
		name = name[index+1:]
	}
	return router.View(name)
}

func (r *Service) Handle(writer http.ResponseWriter, request *http.Request) {
	err := r.handle(writer, request)
	if err != nil {
		http.Error(writer, err.Error(), http.StatusInternalServerError)
	}
}

func (r *Service) handle(writer http.ResponseWriter, request *http.Request) error {
	err := r.reloadResourceIfNeeded(context.Background())
	if err != nil {
		return err
	}
	URI := request.RequestURI
	if strings.Contains(URI, "://") {
		_, URI = furl.Base(URI, "https")
	}

	if request.URL == nil {
		host := os.Getenv("FUNCTION_NAME")
		if host == "" {
			host = request.Host
		}
		if host == "" {
			host = "localhost"
		}
		URL := "https://" + host + "/" + URI
		request.URL, err = url.Parse(URL)
	}
	if index := strings.Index(URI, r.Config.APIPrefix); index != -1 {
		URI = URI[index+len(r.Config.APIPrefix):]
		request.RequestURI = r.Config.APIPrefix + URI
	}
	routePath := URI
	if idx := strings.Index(URI, "?"); idx != -1 {
		routePath = URI[:idx]
	}
	router, err := r.match(routePath)
	if err == nil {
		err = router.Handle(writer, request)
		if err != nil {
			err = fmt.Errorf("failed to route: %v, %v, %v %w", request.Method, request.RequestURI, request.URL.String(), err)
		}
	}
	return err
}

func (r *Service) reloadFs() afs.Service {
	if r.Config.UseCacheFS {
		return r.cfs
	}
	return r.fs
}

func (r *Service) match(URI string) (*router.Router, error) {
	r.mux.RLock()
	routes := r.routers
	r.mux.RUnlock()
	parts := strings.Split(URI, "/")
	for i := len(parts); i > 0; i-- {
		key := strings.Join(parts[:i], "/")
		result, ok := routes[key]
		if ok {
			return result, nil
		}
	}
	return nil, fmt.Errorf("failed to match APIURI: %v", r.Config.APIPrefix+URI)
}

func (r *Service) reloadResourceIfNeeded(ctx context.Context) error {
	if err := r.reloadDataResourceIfNeeded(ctx); err != nil {
		log.Printf("failed to reload view reousrces: %v", err)
	}
	return r.reloadRouterResourcesIfNeeded(ctx)
}

func (r *Service) reloadRouterResourcesIfNeeded(ctx context.Context) error {
	fs := r.reloadFs()
	var resourcesSnapshot = map[string]*router.Resource{}
	hasChanged := false

	err := r.routeResourceTracker.Notify(ctx, fs, r.handleRouterResourceChange(ctx, &hasChanged, resourcesSnapshot, fs))
	if err != nil || !hasChanged {
		return err
	}
	var updatedResource []*router.Resource
	routers := map[string]*router.Router{}
	for k := range resourcesSnapshot {
		item := resourcesSnapshot[k]
		key := strings.Trim(item.APIURI, "/")
		if _, ok := routers[key]; ok {
			return fmt.Errorf("duplicate resource APIURI: %v,-> %v", key, item.SourceURL)
		}
		routers[key] = router.New(item)
		updatedResource = append(updatedResource, item)
	}
	r.mux.Lock()
	defer r.mux.Unlock()
	r.routerResources = updatedResource
	r.routers = routers
	return nil
}

func (r *Service) handleRouterResourceChange(ctx context.Context, hasChanged *bool, resourcesSnapshot map[string]*router.Resource, fs afs.Service) func(URL string, operation resource.Operation) {
	return func(URL string, operation resource.Operation) {
		*hasChanged = true
		if len(resourcesSnapshot) == 0 {
			r.mux.RLock()
			for i, item := range r.routerResources {
				resourcesSnapshot[item.SourceURL] = r.routerResources[i]
			}
			r.mux.RUnlock()
		}
		switch operation {
		case resource.Added, resource.Modified:
			if strings.HasSuffix(URL, "sql") {
				return
			}
			res, err := r.loadRouterResource(ctx, URL, fs)
			if err != nil {
				log.Printf("failed to load %v, %v\n", URL, err)
				return
			}
			resourcesSnapshot[res.SourceURL] = res

		case resource.Deleted:
			delete(resourcesSnapshot, URL)
		}
	}
}

func (r *Service) reloadDataResourceIfNeeded(ctx context.Context) error {
	fs := r.reloadFs()
	var resourcesSnapshot = map[string]*view.Resource{}
	hasChanged := false
	err := r.dataResourceTracker.Notify(ctx, fs, r.handleDataResourceChange(ctx, &hasChanged, resourcesSnapshot, fs))
	if err != nil || !hasChanged {
		return err
	}
	r.mux.Lock()
	defer r.mux.Unlock()
	r.dataResources = resourcesSnapshot
	return nil
}

func (r *Service) handleDataResourceChange(ctx context.Context, hasChanged *bool, resourcesSnapshot map[string]*view.Resource, fs afs.Service) func(URL string, operation resource.Operation) {
	return func(URL string, operation resource.Operation) {
		_, key := furl.Split(URL, file.Scheme)
		if index := strings.Index(key, "."); index != -1 {
			key = key[:index]
		}
		*hasChanged = true
		if len(resourcesSnapshot) == 0 {
			r.mux.RLock()
			for i, item := range r.dataResources {
				resourcesSnapshot[item.SourceURL] = r.dataResources[i]
			}
			r.mux.RUnlock()
		}
		switch operation {
		case resource.Added, resource.Modified:
			res, err := view.LoadResourceFromURL(ctx, URL, fs)
			if err != nil {
				log.Printf("failed to load %v, %v\n", URL, err)
				return
			}
			resourcesSnapshot[key] = res
		case resource.Deleted:
			delete(resourcesSnapshot, URL)
		}
	}
}

func (r *Service) loadRouterResource(ctx context.Context, URL string, fs afs.Service) (*router.Resource, error) {
	types := view.Types{}
	for k, v := range r.types {
		types[k] = v
	}

	var metrics *view.Metrics
	if r.metrics != nil {
		appURI := r.apiURI(URL)
		URIPart, _ := path.Split(appURI)
		metrics = &view.Metrics{
			Service: r.metrics,
			URIPart: URIPart,
		}
	}
	resource, err := router.NewResourceFromURL(ctx, fs, URL, r.visitors, types, r.dataResources, metrics)
	if err != nil {
		return nil, err
	}
	if err = r.initResource(ctx, resource, URL); err != nil {
		return nil, err
	}
	return resource, nil
}

func (r *Service) initResource(ctx context.Context, resource *router.Resource, URL string) error {
	resource.SourceURL = URL
	if resource.APIURI == "" {
		appURI := r.apiURI(URL)
		resource.APIURI = appURI
	}
	return resource.Init(ctx)
}

func (r *Service) apiURI(URL string) string {
	appURI := strings.Trim(URL[len(r.Config.RouteURL):], "/")
	if index := strings.Index(appURI, "."); index != -1 {
		appURI = appURI[:index]
	}
	return appURI
}

//New creates a gateway service
func New(ctx context.Context, config *Config, visitors visitor.Visitors, types view.Types, metrics *gmetric.Service) (*Service, error) {
	config.Init()
	err := config.Validate()
	if err != nil {
		return nil, err
	}
	URL, _ := furl.Split(config.RouteURL, file.Scheme)
	srv := &Service{
		visitors:             visitors,
		metrics:              metrics,
		types:                types,
		Config:               config,
		mux:                  sync.RWMutex{},
		fs:                   afs.New(),
		cfs:                  cache.Singleton(URL),
		dataResources:        map[string]*view.Resource{},
		routeResourceTracker: resource.New(config.RouteURL, time.Duration(config.SyncFrequencyMs)*time.Millisecond),
		dataResourceTracker:  resource.New(config.DependencyURL, time.Duration(config.SyncFrequencyMs)*time.Millisecond),
	}
	if err = initSecrets(ctx, config); err != nil {
		return nil, err
	}
	err = srv.reloadResourceIfNeeded(ctx)
	return srv, err
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
