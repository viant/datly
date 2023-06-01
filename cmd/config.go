package cmd

import (
	"context"
	"fmt"
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

	if s.options.PartialConfigURL != "" {
		cfg, err = standalone.NewConfigFromURL(ctx, s.options.PartialConfigURL)
		s.mergeFromPreviousConfig(cfg)
		return cfg, nil
	}

	if s.options.ConfigURL == "" {
		revealMetrics := true
		cfg = &standalone.Config{
			Config: &gateway.Config{
				ExposableConfig: gateway.ExposableConfig{
					APIPrefix:    s.options.ApiURIPrefix,
					RevealMetric: &revealMetrics,
				},
				SensitiveConfig: gateway.SensitiveConfig{
					APIKeys: router.APIKeys{
						{
							URI:    combineURLs(s.options.ApiURIPrefix, s.options.RoutePrefix, "secured"),
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

	s.options.ConfigURL = normalizeURL(s.options.ConfigURL)
	cfg, err = standalone.NewConfigFromURL(ctx, s.options.ConfigURL)
	if cfg.DependencyURL != "" {
		s.options.DependencyURL = cfg.DependencyURL
	}
	return cfg, err
}

func (s *Builder) mergeFromPreviousConfig(cfg *standalone.Config) {
	if cfg == nil {
		return
	}
	if cfg.DependencyURL != "" {
		s.options.DependencyURL = cfg.DependencyURL
	}
	if jwtValidator := cfg.JWTValidator; jwtValidator != nil {
		if s.options.JWTVerifierRSAKey == "" && jwtValidator.RSA != nil {
			s.options.JWTVerifierRSAKey = fmt.Sprintf("%v|%v", jwtValidator.RSA.URL, jwtValidator.RSA.Key)
		}
		if s.options.JWTVerifierHMACKey == "" && jwtValidator.HMAC != nil {
			s.options.JWTVerifierHMACKey = fmt.Sprintf("%v|%v", jwtValidator.HMAC.URL, jwtValidator.HMAC.Key)
		}
	}
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

	if s.options.PartialConfigURL != "" {
		if connectorResource, _ := loadResource(s.options.DependencyURL, "connections.yaml"); connectorResource != nil {
			s.options.SetConnectors(append(s.options.Connectors(), connectorResource.Connectors...))
		}
		if constantResource, _ := loadResource(s.options.DependencyURL, constFileName+".yaml"); constantResource != nil {
			s.constFileContent.MergeFrom(constantResource.Parameters...)
		}
		var constMap = map[string]interface{}{}
		if err := s.loadConstants(s.options.ConstURL, &constMap); err != nil {
			return err
		}
		if len(constMap) > 0 {
			for k, v := range constMap {
				s.constFileContent.AddConst(k, v)
			}
		}
	}

	if s.options.RouteURL != "" {
		cfg.RouteURL = s.options.RouteURL
	} else if cfg.RouteURL != "" {
		cfg.RouteURL = normalizeURL(cfg.RouteURL)
		cfg.DependencyURL = normalizeURL(cfg.DependencyURL)
	}

	s.initJWTVerifier(cfg)

	err := buildDefaultConfig(cfg, s.options)
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
		cfg.URL = fmt.Sprintf("mem://localhost/%s/Datly/config.json", options.RoutePrefix)
		if cfg.RouteURL == "" {
			cfg.RouteURL = fmt.Sprintf("mem://localhost/%s/Datly/routes", options.RoutePrefix)
			fs.Create(context.Background(), cfg.RouteURL, file.DefaultDirOsMode, true)
			options.RouteURL = cfg.RouteURL
		}
		if cfg.PluginsURL == "" {
			cfg.PluginsURL = fmt.Sprintf("mem://localhost/%s/Datly/plugins", options.RoutePrefix)
			_ = fs.Create(context.Background(), cfg.RouteURL, file.DefaultDirOsMode, true)
			options.PluginsURL = cfg.PluginsURL
		}

		if cfg.AssetsURL == "" {
			cfg.AssetsURL = fmt.Sprintf("mem://localhost/%s/Datly/assets", options.RoutePrefix)
			_ = fs.Create(context.Background(), cfg.AssetsURL, file.DefaultDirOsMode, true)
		}
		if cfg.DependencyURL == "" {
			if options.AssetsURL == "" {
				options.AssetsURL = fmt.Sprintf("mem://localhost/%s/Datly/dependencies", options.RoutePrefix)
			}
			cfg.DependencyURL = options.AssetsURL
			_ = fs.Create(context.Background(), cfg.DependencyURL, file.DefaultDirOsMode, true)
		}
		cfg.Meta.Init()
		if !strings.HasPrefix(cfg.Meta.MetricURI, options.ApiURIPrefix) {
			cfg.Meta.MetricURI = strings.Replace(cfg.Meta.MetricURI, APIPrefix, options.ApiURIPrefix, 1)
		}
		if !strings.HasPrefix(cfg.Meta.StatusURI, options.ApiURIPrefix) {
			cfg.Meta.StatusURI = strings.Replace(cfg.Meta.StatusURI, APIPrefix, options.ApiURIPrefix, 1)
		}
		if !strings.HasPrefix(cfg.Meta.ConfigURI, options.ApiURIPrefix) {
			cfg.Meta.ConfigURI = strings.Replace(cfg.Meta.ConfigURI, APIPrefix, options.ApiURIPrefix, 1)
		}
		if !strings.HasPrefix(cfg.Meta.ViewURI, options.ApiURIPrefix) {
			cfg.Meta.ViewURI = strings.Replace(cfg.Meta.ViewURI, APIPrefix, options.ApiURIPrefix, 1)
		}
		if !strings.HasPrefix(cfg.Meta.OpenApiURI, options.ApiURIPrefix) {
			cfg.Meta.OpenApiURI = strings.Replace(cfg.Meta.OpenApiURI, APIPrefix, options.ApiURIPrefix, 1)
		}
		if !strings.HasPrefix(cfg.Meta.CacheWarmURI, options.ApiURIPrefix) {
			cfg.Meta.CacheWarmURI = strings.Replace(cfg.Meta.CacheWarmURI, APIPrefix, options.ApiURIPrefix, 1)
		}
		if !strings.HasPrefix(cfg.Meta.StructURI, options.ApiURIPrefix) {
			cfg.Meta.StructURI = strings.Replace(cfg.Meta.StructURI, APIPrefix, options.ApiURIPrefix, 1)
		}
		if err := fsAddJSON(fs, cfg.URL, cfg); err != nil {
			return err
		}
		options.ConfigURL = cfg.URL
	}
	return nil
}

func loadResource(dependencyURL, resourceName string) (*view.Resource, error) {
	if dependencyURL != "" {
		connectorURL := url.Join(dependencyURL, resourceName)
		return view.LoadResourceFromURL(context.Background(), connectorURL, afs.New())
	}
	return nil, nil
}
