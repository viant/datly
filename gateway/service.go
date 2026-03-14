package gateway

import (
	"context"
	"fmt"
	"github.com/viant/afs"
	"github.com/viant/afs/cache"
	"github.com/viant/afs/file"
	"github.com/viant/afs/matcher"
	"github.com/viant/afs/option"
	furl "github.com/viant/afs/url"
	"github.com/viant/datly/internal/debuglog"
	"github.com/viant/datly/repository"
	"github.com/viant/datly/repository/locator/component/dispatcher"
	"github.com/viant/datly/view"
	"github.com/viant/datly/view/extension"
	"github.com/viant/gmetric"
	serverproto "github.com/viant/mcp-protocol/server"
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
		mcpRegistry   *serverproto.Registry
	}
)

func (r *Service) MCP() *serverproto.Registry {
	if r == nil {
		return nil
	}
	if r.mcpRegistry == nil {
		return nil
	}
	return r.mcpRegistry
}

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
	//start := time.Now()
	options, err := newOptions(ctx, opts...)
	if err != nil {
		return nil, err
	}
	aConfig := options.config

	if err := aConfig.Init(ctx); err != nil {
		return nil, err
	}
	if options.repository == nil {
		if err := aConfig.Validate(); err != nil {
			return nil, err
		}
	} else {
		if aConfig.DQLBootstrap != nil && len(aConfig.DQLBootstrap.Sources) == 0 {
			return nil, fmt.Errorf("DQLBootstrap.Sources was empty")
		}
		if err := validateAsyncJobPaths(aConfig.JobURL, aConfig.FailedJobURL); err != nil {
			return nil, err
		}
	}
	fs, err := newFileService(aConfig)
	if err != nil {
		return nil, err
	}
	componentRepository, mainRouter, mcpRegistry, err := buildServiceRuntime(ctx, aConfig, options.repository, nil, options.extensions, options.metrics, options.statusHandler, options.refreshDisabled)
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
		mcpRegistry:   mcpRegistry,
	}

	go srv.watchAsyncJob(context.Background())
	//fmt.Printf("[INFO]: started gatweay after: %s\n", time.Since(start))
	return srv, err
}

func (r *Service) ReloadDQLSources(ctx context.Context, sources []string) error {
	if r == nil {
		return fmt.Errorf("gateway service: nil")
	}
	cfg := cloneConfig(r.Config)
	if cfg.DQLBootstrap == nil {
		cfg.DQLBootstrap = &DQLBootstrap{}
	}
	cfg.DQLBootstrap.Sources = append([]string{}, sources...)
	componentRepository, mainRouter, mcpRegistry, err := buildServiceRuntime(ctx, cfg, nil, r.repository.Resources(), r.repository.Extensions(), r.metrics, r.statusHandler, true)
	if err != nil {
		return err
	}
	r.mux.Lock()
	r.Config = cfg
	r.repository = componentRepository
	r.mainRouter = mainRouter
	r.mcpRegistry = mcpRegistry
	r.mux.Unlock()
	return nil
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
			//fmt.Printf(format, args...)
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
	mainRouter, err := NewRouter(ctx, r.repository, r.Config, metrics, statusHandler, r.mcpRegistry)
	if err != nil {
		return err
	}
	r.mux.Lock()
	newCount := len(mainRouter.paths)
	oldCount := 0
	if r.mainRouter != nil {
		oldCount = len(r.mainRouter.paths)
	}
	if newCount < oldCount {
		r.mux.Unlock()
		fmt.Printf("[INFO]: routers rebuild skipped (new config has %d routes vs %d existing, keeping existing)\n", newCount, oldCount)
		return nil
	}
	fmt.Printf("[INFO]: router replacing old(%d routes) with new(%d routes)\n", oldCount, newCount)
	r.mainRouter = mainRouter
	r.mux.Unlock()
	fmt.Printf("[INFO]: routers rebuild completed after: %s\n", time.Since(start))
	return nil
}

func buildServiceRuntime(ctx context.Context, cfg *Config, repo *repository.Service, resources repository.Resources, extensions *extension.Registry, metrics *gmetric.Service, statusHandler http.Handler, refreshDisabled bool) (*repository.Service, *Router, *serverproto.Registry, error) {
	componentRepository := repo
	var err error
	if componentRepository == nil {
		componentURL := effectiveRouteURL(cfg)
		var repoFS afs.Service
		if hasNonFileScheme(componentURL) {
			repoFS = afs.New()
			if createErr := repoFS.Create(ctx, componentURL, file.DefaultDirOsMode, true); createErr != nil {
				return nil, nil, nil, fmt.Errorf("failed to initialize bootstrap route store %s: %w", componentURL, createErr)
			}
		}
		options := []repository.Option{
			repository.WithComponentURL(componentURL),
			repository.WithResourceURL(cfg.DependencyURL),
			repository.WithPluginURL(cfg.PluginsURL),
			repository.WithApiPrefix(cfg.APIPrefix),
			repository.WithResources(resources),
			repository.WithExtensions(extensions),
			repository.WithMetrics(metrics),
			repository.WithJWTSigner(cfg.JwtSigner),
			repository.WithJWTVerifier(cfg.JWTValidator),
			repository.WithCognitoAuth(cfg.Cognito),
			repository.WithFirebaseAuth(cfg.Firebase),
			repository.WithDependencyURL(cfg.DependencyURL),
			repository.WithRefreshFrequency(cfg.SyncFrequency()),
			repository.WithRefreshDisabled(refreshDisabled),
			repository.WithDispatcher(dispatcher.New),
		}
		if repoFS != nil {
			options = append(options, repository.WithFS(repoFS))
		}
		componentRepository, err = repository.New(ctx, options...)
		if err != nil {
			debuglog.JSON("gateway.build_service_runtime.repository_new_error", map[string]any{
				"routeURL":        componentURL,
				"dependencyURL":   cfg.DependencyURL,
				"pluginsURL":      cfg.PluginsURL,
				"apiPrefix":       cfg.APIPrefix,
				"hasDQL":          cfg.hasDQLBootstrap(),
				"hasGo":           cfg.hasGoBootstrap(),
				"refreshDisabled": refreshDisabled,
				"error":           err.Error(),
			})
			return nil, nil, nil, fmt.Errorf("failed to initialise component service: %w", err)
		}
	}
	if err = (&Service{Config: cfg}).applyDQLBootstrap(ctx, componentRepository, cfg.DQLBootstrap); err != nil {
		return nil, nil, nil, fmt.Errorf("failed to apply DQL bootstrap: %w", err)
	}
	if err = (&Service{Config: cfg}).applyGoBootstrap(ctx, componentRepository, cfg.GoBootstrap); err != nil {
		return nil, nil, nil, fmt.Errorf("failed to apply Go bootstrap: %w", err)
	}
	var mcpRegistry *serverproto.Registry
	if cfg.MCP != nil {
		mcpRegistry = serverproto.NewRegistry()
	}
	mainRouter, err := NewRouter(ctx, componentRepository, cfg, metrics, statusHandler, mcpRegistry)
	if err != nil {
		return nil, nil, nil, err
	}
	return componentRepository, mainRouter, mcpRegistry, nil
}

func effectiveRouteURL(cfg *Config) string {
	if cfg == nil {
		return ""
	}
	if cfg.RouteURL != "" {
		return cfg.RouteURL
	}
	if cfg.hasDQLBootstrap() || cfg.hasGoBootstrap() {
		return "mem://datly/routes"
	}
	return ""
}

func cloneConfig(cfg *Config) *Config {
	if cfg == nil {
		return &Config{}
	}
	cloned := *cfg
	if cfg.DQLBootstrap != nil {
		boot := *cfg.DQLBootstrap
		boot.Sources = append([]string{}, cfg.DQLBootstrap.Sources...)
		boot.Exclude = append([]string{}, cfg.DQLBootstrap.Exclude...)
		cloned.DQLBootstrap = &boot
	}
	if cfg.GoBootstrap != nil {
		boot := *cfg.GoBootstrap
		boot.Packages = append([]string{}, cfg.GoBootstrap.Packages...)
		boot.Exclude = append([]string{}, cfg.GoBootstrap.Exclude...)
		cloned.GoBootstrap = &boot
	}
	if cfg.MCP != nil {
		mcp := *cfg.MCP
		cloned.MCP = &mcp
	}
	return &cloned
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
