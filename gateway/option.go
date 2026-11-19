package gateway

import (
	"context"
	"embed"
	"github.com/viant/datly/repository"
	"github.com/viant/datly/view/extension"
	"github.com/viant/gmetric"
	"net/http"
)

type options struct {
	authorizationProviders map[string]AuthorizationProvider
	config                 *Config
	initializers           []func(config *Config, fs *embed.FS) error
	extensions             *extension.Registry
	metrics                *gmetric.Service
	repository             *repository.Service
	statusHandler          http.Handler
	embedFs                *embed.FS
	configURL              string
	refreshDisabled        bool
}

func newOptions(ctx context.Context, opts ...Option) (*options, error) {
	result := &options{}
	for _, option := range opts {
		option(result)
	}
	if result.metrics == nil {
		result.metrics = gmetric.New()
	}
	if ext := result.extensions; ext == nil {
		result.extensions = extension.NewRegistry()
	}

	if result.config == nil {
		if result.configURL != "" {
			var err error

			if result.config, err = NewConfigFromURL(ctx, fs, result.configURL); err != nil {
				return nil, err
			}
		}
	}
	if result.config == nil {
		result.config = &Config{}
	}

	if result.authorizationProviders != nil {
		copy := *result.config
		copy.AuthorizationProviders = make(map[string]AuthorizationProvider, len(result.authorizationProviders))
		for name, provider := range result.authorizationProviders {
			copy.AuthorizationProviders[name] = provider
		}
		result.config = &copy
	}
	return result, nil
}

// Option represents a service option
type Option func(*options)

// WithConfig sets a config
func WithConfig(config *Config) Option {
	return func(o *options) {
		o.config = config
	}
}

// WithExtensions sets an extension registry
func WithExtensions(registry *extension.Registry) Option {
	return func(o *options) {
		o.extensions = registry
	}
}

// WithMetrics sets a metrics service
func WithMetrics(metrics *gmetric.Service) Option {
	return func(o *options) {
		o.metrics = metrics
	}
}

// WithRepository sets a repository service
func WithRepository(repository *repository.Service) Option {
	return func(o *options) {
		o.repository = repository
	}
}

// WithEmbedFs sets an embed file system
func WithEmbedFs(embedFs *embed.FS) Option {
	return func(o *options) {
		o.embedFs = embedFs
	}
}

func WithStatusHandler(handler http.Handler) Option {
	return func(o *options) {
		o.statusHandler = handler
	}
}

func WithInitializer(initializer func(config *Config, fs *embed.FS) error) Option {
	return func(o *options) {
		o.initializers = append(o.initializers, initializer)
	}
}

func WithConfigURL(configURL string) Option {
	return func(o *options) {
		o.configURL = configURL
	}
}

func WithRefreshDisabled(enabled bool) Option {
	return func(o *options) {
		o.refreshDisabled = enabled
	}
}

// WithAuthorizationProviders registers trusted process-owned identity adapters
// when configuration is loaded from a file. Provider names are operator-owned.
func WithAuthorizationProviders(providers map[string]AuthorizationProvider) Option {
	return func(o *options) { o.authorizationProviders = providers }
}
