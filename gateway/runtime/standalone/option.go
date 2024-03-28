package standalone

import (
	"context"
	"embed"
	"github.com/viant/datly/gateway"
	"github.com/viant/datly/service/auth/jwt"
)

// Options represents standalone options
type Options struct {
	ConfigURL    string `short:"c" long:"cfg" description:"config URIPrefix"`
	Version      bool   `short:"v" long:"version" description:"Version"`
	config       *Config
	options      []gateway.Option
	useSingleton *bool
}

func (o *Options) UseSingleton() bool {
	if o.useSingleton == nil {
		return true
	}
	return *o.useSingleton
}

func NewOptions(ctx context.Context, opts ...Option) (*Options, error) {
	options := &Options{}
	for _, opt := range opts {
		opt(options)
	}
	options.options = append(options.options, gateway.WithAuthProvider(func(config *gateway.Config, fs *embed.FS) (gateway.Authorizer, error) {
		return jwt.Init(config, fs)
	}))
	if options.config != nil {
		options.options = append(options.options, gateway.WithConfig(options.config.Config))
	} else if options.ConfigURL == "" {
		options.options = append(options.options, gateway.WithConfigURL(options.ConfigURL))
	}
	return options, nil
}

// Option represents standalone option
type Option func(*Options)

// WithOptions sets options
func WithOptions(options ...gateway.Option) Option {
	return func(o *Options) {
		o.options = options
	}
}

// WithUseSingleton sets a singleton
func WithUseSingleton(useSingleton bool) Option {
	return func(o *Options) {
		o.useSingleton = &useSingleton
	}
}

func WithConfig(config *Config) Option {
	return func(o *Options) {
		o.config = config
	}
}

func WithConfigURL(configURL string) Option {
	return func(o *Options) {
		o.ConfigURL = configURL
	}
}
