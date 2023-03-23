package cmd

import (
	"context"
	"encoding/json"
	"github.com/viant/afs"
	"github.com/viant/afs/file"
	"github.com/viant/afs/url"
	"github.com/viant/datly/gateway"
	"github.com/viant/datly/gateway/runtime/standalone"
	"github.com/viant/datly/gateway/runtime/standalone/endpoint"
	"github.com/viant/datly/router"
	"github.com/viant/datly/view"
	"github.com/viant/scy"
	"github.com/viant/scy/auth/jwt/verifier"
	"strings"
)

func (s *Builder) loadConfig(ctx context.Context) (cfg *standalone.Config, err error) {
	if s.options.ConfigURL == "" {
		revealMetrics := true
		cfg = &standalone.Config{
			Config: &gateway.Config{
				ExposableConfig: gateway.ExposableConfig{
					APIPrefix:    "/v1/api/",
					RevealMetric: &revealMetrics,
				},
				SensitiveConfig: gateway.SensitiveConfig{
					APIKeys: router.APIKeys{
						{
							URI:    "/v1/api/dev/secured/",
							Header: "App-Secret-Id",
							Value:  "changeme",
						},
					},
				},
			},
			Endpoint: endpoint.Config{},
		}
		cfg.Init()
		disable := true
		cfg.AutoDiscovery = &disable
		return cfg, nil
	}

	if s.options.PartialConfigURL != "" {
		configContent, err := s.fs.DownloadWithURL(ctx, s.options.PartialConfigURL)
		if err != nil {
			return nil, err
		}

		if err = json.Unmarshal(configContent, cfg); err != nil {
			return nil, err
		}
	}

	s.options.ConfigURL = normalizeURL(s.options.ConfigURL)
	cfg, err = standalone.NewConfigFromURL(ctx, s.options.ConfigURL)

	return cfg, err
}

func (s *Builder) initConfig(ctx context.Context, cfg *standalone.Config) error {
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

	s.initJWTVerifier(cfg)

	err = buildDefaultConfig(cfg, s.options)
	if err != nil {
		return err
	}
	if cfg.DependencyURL != "" && s.options.DependencyURL == "" {
		s.options.DependencyURL = cfg.DependencyURL
	}

	return nil
}

func (s *Builder) initJWTVerifier(cfg *standalone.Config) {
	if s.options.JWTVerifierRSAKey == "" && s.options.JWTVerifierHMACKey == "" {
		return
	}
	cfg.JWTValidator = &verifier.Config{}
	if s.options.JWTVerifierRSAKey != "" {
		cfg.JWTValidator.RSA = getScyResource(s.options.JWTVerifierRSAKey)
	}
	if s.options.JWTVerifierHMACKey != "" {
		cfg.JWTValidator.HMAC = getScyResource(s.options.JWTVerifierHMACKey)
	}
}

func getScyResource(location string) *scy.Resource {
	pair := strings.Split(location, "|")
	var result *scy.Resource
	switch len(pair) {
	case 2:
		result = &scy.Resource{URL: pair[0], Key: pair[1]}
	default:
		result = &scy.Resource{URL: pair[0]}
	}
	URL := result.URL
	if url.Scheme(URL, "e") == "e" && URL[0] != '/' {
		result.URL = normalizeURL(URL)
	}
	return result
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

		if cfg.PluginsURL == "" {
			cfg.PluginsURL = "mem://localhost/dev/Datly/plugins"
			fs.Create(context.Background(), cfg.RouteURL, file.DefaultDirOsMode, true)
			options.PluginsURL = cfg.PluginsURL
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
