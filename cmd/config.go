package cmd

import (
	"context"
	"github.com/viant/afs"
	"github.com/viant/afs/file"
	"github.com/viant/datly/gateway"
	"github.com/viant/datly/gateway/runtime/standalone"
	"github.com/viant/datly/gateway/runtime/standalone/endpoint"
	"github.com/viant/datly/view"
)

func loadConfig(ctx context.Context, options *Options) (cfg *standalone.Config, err error) {
	if options.ConfigURL == "" {
		cfg = &standalone.Config{
			Config: &gateway.Config{
				APIPrefix: "/v1/api/",
			},
			Endpoint: endpoint.Config{},
		}
		cfg.Init()
		return cfg, nil
	}
	options.ConfigURL = normalizeURL(options.ConfigURL)
	cfg, err = standalone.NewConfigFromURL(ctx, options.ConfigURL)
	return cfg, err
}

func initConfig(cfg *standalone.Config, options *Options) error {
	if options.Port != 0 {
		cfg.Endpoint.Port = options.Port
	}
	if URL := options.RouteURL; URL != "" {
		cfg.RouteURL = URL
	}
	if URL := options.DependencyURL; URL != "" {
		cfg.DependencyURL = URL
	}
	cfg.Init()
	connectors, err := loadConnectors(cfg, options)
	if err != nil {
		return err
	}
	if options.RouteURL != "" {
		cfg.RouteURL = options.RouteURL
	}
	err = buildDefaultConfig(cfg, options)
	if err != nil {
		return err
	}
	return buildViewWithRouter(options, cfg, connectors)
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
