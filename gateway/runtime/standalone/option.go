package standalone

import (
	"context"
	"fmt"
	"github.com/viant/datly/gateway"
	"github.com/viant/datly/service/auth/jwt"
)

// Options represents standalone options
type Options struct {
	ConfigURL    string `short:"c" long:"cfg" description:"config URIPrefix"`
	Version      bool   `short:"v" long:"version" description:"Version"`
	config       *Config
	auth         gateway.Authorizer
	useSingleton *bool
}

func NewOptions(ctx context.Context, opts ...Option) (*Options, error) {
	options := &Options{}
	for _, opt := range opts {
		opt(options)
	}
	if options.config == nil {
		if options.ConfigURL == "" {
			return nil, fmt.Errorf("config url was empty")
		}
		ctx = context.Background()
		config, err := NewConfigFromURL(ctx, options.ConfigURL)
		if err != nil {
			return nil, err
		}
		options.config = config
	}
	var err error
	if options.config.Cognito != nil || options.config.JWTValidator != nil {
		if options.auth, err = jwt.Init(options.config.Config, nil); err != nil {
			return nil, err
		}
	}
	return options, nil
}

// Option represents standalone option
type Option func(*Options)

func WithAuth(auth gateway.Authorizer) Option {
	return func(o *Options) {
		o.auth = auth
	}
}

func WithConfig(config *Config) Option {
	return func(o *Options) {
		o.config = config
	}
}

func WithUseSingleton(useSingleton *bool) Option {
	return func(o *Options) {
		o.useSingleton = useSingleton
	}
}

func WithConfigURL(configURL string) Option {
	return func(o *Options) {
		o.ConfigURL = configURL
	}
}
