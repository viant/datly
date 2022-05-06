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
	"log"
	"net/http"
	"strings"
	"sync"
	"time"
)

//Service represents gateway service
type Service struct {
	*Config
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
	err := r.ReloadIfNeeded(context.Background())
	if err != nil {
		return err
	}
	URI := request.RequestURI
	URIPath := URI
	if idx := strings.Index(URI, "?"); idx != -1 {
		URIPath = URI[:idx]
	}
	router, err := r.Match(URIPath)
	if err == nil {
		err = router.Handle(writer, request)
	}
	return err
}

func (r *Service) reloadFs() afs.Service {
	if r.UseCacheFS {
		return r.cfs
	}
	return r.fs
}

func (r *Service) Match(URI string) (*router.Router, error) {
	r.mux.RLock()
	index := r.routers
	r.mux.RUnlock()
	parts := strings.Split(URI, "/")
	for i := len(parts); i > 0; i-- {
		result, ok := index[strings.Join(parts[:i], "/")]
		if ok {
			return result, nil
		}
	}
	return nil, fmt.Errorf("failed to match URI: %v", URI)
}

func (r *Service) ReloadIfNeeded(ctx context.Context) error {
	fs := r.reloadFs()
	var resources map[string]*router.Resource
	hasChanged := false
	err := r.tracker.Notify(ctx, fs, r.handleResourceChange(ctx, &hasChanged, resources, fs))
	if err != nil || !hasChanged {
		return err
	}
	var updatedResource []*router.Resource
	index := map[string]*router.Router{}
	for k := range resources {
		item := resources[k]
		key := strings.Trim(item.URI, "/")
		if _, ok := index[key]; ok {
			return fmt.Errorf("duplicate resource URI: %v,-> %v", key, item.SourceURL)
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

func (r *Service) handleResourceChange(ctx context.Context, hasChanged *bool, resources map[string]*router.Resource, fs afs.Service) func(URL string, operation resource.Operation) {
	return func(URL string, operation resource.Operation) {
		*hasChanged = true
		if len(resources) == 0 {
			resources = make(map[string]*router.Resource)
			r.mux.RLock()
			for i, item := range r.resources {
				resources[item.SourceURL] = r.resources[i]
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
			resources[res.SourceURL] = res
		case resource.Deleted:
			delete(resources, URL)
		}
	}
}

func (r *Service) loadResource(ctx context.Context, URL string, fs afs.Service) (*router.Resource, error) {
	resource, err := router.NewResourceFromURL(ctx, URL, visitors.Visitors{}, data.Types{})
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
	if resource.URI == "" {
		relative := URL[len(r.BaseURL):]
		if index := strings.LastIndex(relative, "."); index != -1 {
			relative = relative[:index-1]
		}
		resource.URI = relative
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
	err = srv.ReloadIfNeeded(ctx)
	return srv, err
}
