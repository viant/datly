package gateway

import (
	"context"
	"fmt"
	"github.com/viant/afs"
	"github.com/viant/afs/cache"
	"github.com/viant/afs/file"
	"github.com/viant/afs/url"
	"github.com/viant/cloudless/resource"
	"github.com/viant/datly/data"
	"github.com/viant/datly/router"
	"github.com/viant/datly/visitor"
	"log"
	"net/http"
	"strings"
	"sync"
	"time"
)

//Service represents gateway service
type Service struct {
	Config    *Config
	mux       sync.RWMutex
	resources []*router.Resource
	routers   map[string]*router.Router
	fs        afs.Service
	cfs       afs.Service //cache file system
	tracker   *resource.Tracker
}

func (r *Service) Handle(writer http.ResponseWriter, request *http.Request) {
	err := r.handle(writer, request)
	if err != nil {
		http.Error(writer, err.Error(), http.StatusInternalServerError)
	}
}

func (r *Service) handle(writer http.ResponseWriter, request *http.Request) error {
	err := r.reloadIfNeeded(context.Background())
	if err != nil {
		return err
	}
	URI := request.RequestURI
	if strings.Contains(URI, "://") {
		_, URI = url.Base(URI, "https")
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
	index := r.routers
	r.mux.RUnlock()

	parts := strings.Split(URI, "/")
	for i := len(parts); i > 0; i-- {
		key := strings.Join(parts[:i], "/")
		result, ok := index[key]
		if ok {
			return result, nil
		}
	}
	return nil, fmt.Errorf("failed to match APIURI: %v", r.Config.APIPrefix+URI)
}

func (r *Service) reloadIfNeeded(ctx context.Context) error {
	fs := r.reloadFs()
	var resourcesSnapshot = map[string]*router.Resource{}
	hasChanged := false
	err := r.tracker.Notify(ctx, fs, r.handleResourceChange(ctx, &hasChanged, resourcesSnapshot, fs))
	if err != nil || !hasChanged {
		return err
	}
	var updatedResource []*router.Resource
	index := map[string]*router.Router{}
	for k := range resourcesSnapshot {
		item := resourcesSnapshot[k]
		key := strings.Trim(item.APIURI, "/")
		if _, ok := index[key]; ok {
			return fmt.Errorf("duplicate resource APIURI: %v,-> %v", key, item.SourceURL)
		}
		index[key] = router.New(item)
		updatedResource = append(updatedResource, item)
	}
	r.mux.Lock()
	defer r.mux.Unlock()
	r.resources = updatedResource
	r.routers = index
	return nil
}

func (r *Service) handleResourceChange(ctx context.Context, hasChanged *bool, resourcesSnapshot map[string]*router.Resource, fs afs.Service) func(URL string, operation resource.Operation) {
	return func(URL string, operation resource.Operation) {
		*hasChanged = true
		if len(resourcesSnapshot) == 0 {
			r.mux.RLock()
			for i, item := range r.resources {
				resourcesSnapshot[item.SourceURL] = r.resources[i]
			}
			r.mux.RUnlock()
		}
		switch operation {
		case resource.Added, resource.Modified:
			res, err := r.loadResource(ctx, URL, fs)
			if err != nil {
				log.Printf("failed to load %v, %v\n", URL, err)
				return
			}
			res.SourceURL = URL
			resourcesSnapshot[res.SourceURL] = res
		case resource.Deleted:
			delete(resourcesSnapshot, URL)
		}
	}
}

func (r *Service) loadResource(ctx context.Context, URL string, fs afs.Service) (*router.Resource, error) {
	resource, err := router.NewResourceFromURL(ctx, fs, URL, visitor.Visitors{}, data.Types{})
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
		appURI := strings.Trim(URL[len(r.Config.BaseURL):], "/")
		if index := strings.Index(appURI, "."); index != -1 {
			appURI = appURI[:index]
		}
		resource.APIURI = appURI
	}
	return resource.Init(ctx)
}

func New(ctx context.Context, config *Config) (*Service, error) {
	config.Init()
	err := config.Validate()
	if err != nil {
		return nil, err
	}
	URL, _ := url.Split(config.BaseURL, file.Scheme)
	srv := &Service{
		Config:  config,
		mux:     sync.RWMutex{},
		fs:      afs.New(),
		cfs:     cache.Singleton(URL),
		tracker: resource.New(config.BaseURL, time.Duration(config.SyncFrequencyMs)*time.Millisecond),
	}
	err = srv.reloadIfNeeded(ctx)
	return srv, err
}
