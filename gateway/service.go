package gateway

import (
	"context"
	"fmt"
	"github.com/viant/afs"
	"github.com/viant/afs/cache"
	"github.com/viant/afs/matcher"
	"github.com/viant/afs/option"
	"github.com/viant/afs/url"
	"github.com/viant/datly/repository"
	"github.com/viant/datly/repository/locator/component/dispatcher"
	"github.com/viant/datly/view"
	"github.com/viant/gmetric"
	"github.com/viant/scy/auth/jwt/signer"
	"net/http"
	"path"
	"sync"
	"time"
)

type (
	Service struct {
		Config        *Config
		fs            afs.Service
		repository    *repository.Service
		metrics       *gmetric.Service
		mainRouter    *Router
		cancelFn      context.CancelFunc
		mux           sync.RWMutex
		statusHandler http.Handler
	}
)

func (r *Service) JWTSigner() *signer.Service {
	return r.repository.JWTSigner()
}

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
	return aRouter, writer, true
}

func (r *Service) Router() (*Router, bool) {
	if err := r.syncChanges(context.Background(), r.metrics, r.statusHandler, false); err != nil {
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
func New(ctx context.Context, opts ...Option) (*Service, error) {
	start := time.Now()
	options, err := newOptions(ctx, opts...)
	if err != nil {
		return nil, err
	}
	aConfig := options.config

	if err := aConfig.Init(ctx); err != nil {
		return nil, err
	}
	fs, err := newFileService(aConfig)
	if err != nil {
		return nil, err
	}
	componentRepository := options.repository
	if componentRepository == nil {

		componentRepository, err = repository.New(ctx, repository.WithComponentURL(aConfig.RouteURL),
			repository.WithResourceURL(aConfig.DependencyURL),
			repository.WithPluginURL(aConfig.PluginsURL),
			repository.WithApiPrefix(aConfig.APIPrefix),
			repository.WithExtensions(options.extensions),
			repository.WithMetrics(options.metrics),
			repository.WithJWTSigner(aConfig.JwtSigner),
			repository.WithJWTVerifier(aConfig.JWTValidator),
			repository.WithCognitoAuth(aConfig.Cognito),
			repository.WithFirebaseAuth(aConfig.Firebase),
			repository.WithCustomAuth(aConfig.Custom),
			repository.WithDependencyURL(aConfig.DependencyURL),
			repository.WithRefreshFrequency(aConfig.SyncFrequency()),
			repository.WithDispatcher(dispatcher.New),
		)
		if err != nil {
			return nil, fmt.Errorf("failed to initialise component service: %w", err)
		}
	}
	mainRouter, err := NewRouter(ctx, componentRepository, aConfig, options.metrics, options.statusHandler)
	if err != nil {
		return nil, err
	}
	srv := &Service{
		metrics:       options.metrics,
		repository:    componentRepository,
		Config:        aConfig,
		mux:           sync.RWMutex{},
		fs:            fs,
		statusHandler: options.statusHandler,
		mainRouter:    mainRouter,
	}
	go srv.watchAsyncJob(context.Background())
	fmt.Printf("[INFO]: started gatweay after: %s\n", time.Since(start))
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
		dir, aPath := url.Split(URL, "file")
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
				return url.Join(base, URL), nil
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

func (r *Service) syncChanges(ctx context.Context, metrics *gmetric.Service, statusHandler http.Handler, isFirst bool) error {
	changed, err := r.repository.SyncChanges(ctx)
	if err != nil {
		return err
	}
	if !changed {
		return nil
	}
	start := time.Now()
	fmt.Printf("[INFO] detected resources changes, rebuilding routers\n")
	mainRouter, err := NewRouter(ctx, r.repository, r.Config, metrics, statusHandler)
	if err != nil {
		return err
	}
	r.mux.Lock()
	r.mainRouter = mainRouter
	r.mux.Unlock()
	fmt.Printf("[INFO]: routers rebuild completed after: %s\n", time.Since(start))
	return nil
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

func (r *Service) PreCachables(ctx context.Context, method string, uri string) ([]*view.View, error) {
	aRouter, ok := r.Router()
	if !ok {
		return []*view.View{}, nil
	}

	return aRouter.PreCacheables(ctx, method, uri)
}
