package cmd

import (
	"context"
	"github.com/viant/afs"
	"github.com/viant/afs/file"
	"github.com/viant/afs/url"
	"github.com/viant/datly/gateway"
	"github.com/viant/datly/gateway/runtime/standalone"
	"github.com/viant/datly/gateway/runtime/standalone/endpoint"
	"github.com/viant/datly/view"
	"github.com/viant/scy"
	"github.com/viant/scy/auth/jwt/verifier"
	"strings"
)

func (s *serverBuilder) loadConfig(ctx context.Context) (cfg *standalone.Config, err error) {
	if s.options.ConfigURL == "" {
		cfg = &standalone.Config{
			Config: &gateway.Config{
				APIPrefix: "/v1/api/",
			},
			Endpoint: endpoint.Config{},
		}
		cfg.Init()
		disable := false
		cfg.AutoDiscovery = &disable
		return cfg, nil
	}

	s.options.ConfigURL = normalizeURL(s.options.ConfigURL)
	cfg, err = standalone.NewConfigFromURL(ctx, s.options.ConfigURL)

	return cfg, err
}

func (s *serverBuilder) initConfig(cfg *standalone.Config) error {
	if s.options.Port != 0 {
		cfg.Endpoint.Port = s.options.Port
	}

	if URL := s.options.RouteURL; URL != "" {
		cfg.RouteURL = normalizeURL(URL)
	}

	if URL := s.options.DependencyURL; URL != "" {
		cfg.DependencyURL = normalizeURL(URL)
	}

	cfg.Init()

	_, err := loadConnectors(cfg, s.options)
	if err != nil {
		return err
	}

	if s.options.RouteURL != "" {
		cfg.RouteURL = s.options.RouteURL
	} else if cfg.RouteURL != "" {
		cfg.RouteURL = normalizeURL(cfg.RouteURL)
		cfg.DependencyURL = normalizeURL(cfg.DependencyURL)
	}

	if s.options.JWTVerifier != "" {
		pair := strings.Split(s.options.JWTVerifier, "|")
		switch len(pair) {
		case 1:
			cfg.JWTValidator = &verifier.Config{RSA: &scy.Resource{URL: pair[0]}}
		case 2:
			cfg.JWTValidator = &verifier.Config{RSA: &scy.Resource{URL: pair[0], Key: pair[1]}}
		}
		if cfg.JWTValidator != nil && cfg.JWTValidator.RSA != nil {
			URL := cfg.JWTValidator.RSA.URL
			if url.Scheme(URL, "e") == "e" && URL[0] != '/' {
				cfg.JWTValidator.RSA.URL = normalizeURL(URL)
			}
		}
	}

	err = buildDefaultConfig(cfg, s.options)
	if err != nil {
		return err
	}
	if cfg.DependencyURL != "" && s.options.DependencyURL == "" {
		s.options.DependencyURL = cfg.DependencyURL
	}

	return s.buildViewWithRouter(cfg)
}

func buildDefaultConfig(cfg *standalone.Config, options *Options) error {
	fs := afs.New()
	if cfg.URL == "" {
		cfg.URL = "mem://localhost/dev/Datly/config.json"
		if cfg.RouteURL == "" {
			cfg.RouteURL = "mem://localhost/dev/Datly/routes"
			fs.Create(context.Background(), cfg.RouteURL, file.DefaultDirOsMode, true)
			options.RouteURL = cfg.RouteURL
		}
		if cfg.DependencyURL == "" {
			cfg.DependencyURL = "mem://localhost/dev/Datly/dependencies"
			fs.Create(context.Background(), cfg.DependencyURL, file.DefaultDirOsMode, true)
		}
		if err := fsAddJSON(fs, cfg.URL, cfg); err != nil {
			return err
		}
		options.ConfigURL = cfg.URL
	}
	return nil
}

func loadConnectors(cfg *standalone.Config, options *Options) (map[string]*view.Connector, error) {
	var resource *view.Resource
	var connectors = make(map[string]*view.Connector)
	var err error
	if options.DependencyURL != "" {
		if resource, err = view.LoadResourceFromURL(context.Background(), options.DependencyURL, afs.New()); err != nil {
			return nil, err
		}
		connectors = resource.ConnectorByName()
		cfg.DependencyURL = options.DependencyURL
	}
	return connectors, err
}
