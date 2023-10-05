package translator

import (
	"context"
	"encoding/json"
	"github.com/viant/afs"
	"github.com/viant/afs/file"
	"github.com/viant/afs/url"
	"github.com/viant/datly/cmd/options"
	"github.com/viant/datly/gateway"
	"github.com/viant/datly/gateway/runtime/standalone"
	"github.com/viant/datly/gateway/runtime/standalone/endpoint"
	"github.com/viant/datly/internal/setter"
	"github.com/viant/datly/internal/translator/parser"
	dpath "github.com/viant/datly/repository/path"
	"os"
	"path"
	"strings"
)

var fs = afs.New()

type Config struct {
	repository *options.Repository
	*standalone.Config
}

func (c *Config) Init(ctx context.Context) error {
	if len(c.repository.Configs) == 0 {
		c.Config = c.inMemoryConfig()
	} else if err := c.loadConfig(ctx); err != nil {
		return err
	}
	if err := c.updateURIs(); err != nil {
		return err
	}
	c.updateOauth(ctx)
	return nil
}

func (c *Config) BaseURL() string {

	if c.repository.RepositoryURL == "" {
		c.repository.RepositoryURL = c.repository.Configs.Repository()
	}
	if url.IsRelative(c.repository.RepositoryURL) {
		if c.repository.ProjectURL == "" {
			c.repository.ProjectURL, _ = os.Getwd()
		}
		c.repository.RepositoryURL = url.Join(c.repository.ProjectURL, c.repository.RepositoryURL)
	}

	if c.repository.RepositoryURL != "" {
		return url.Join(c.repository.RepositoryURL, "Datly")
	}
	dir, _ := os.Getwd()
	return url.Join(dir, "Datly")
}

func (c *Config) updateURIs() error {
	cfg := c.Config
	baseURL := c.BaseURL()

	if c.Config.URL == "" {
		c.Config.URL = url.Join(baseURL, "config.json")
	}
	if cfg.RouteURL == "" {
		cfg.RouteURL = url.Join(baseURL, "routes")
		_ = fs.Create(context.Background(), cfg.RouteURL, file.DefaultDirOsMode, true)
	}
	if cfg.PluginsURL == "" {
		cfg.PluginsURL = url.Join(baseURL, "plugins")
		_ = fs.Create(context.Background(), cfg.RouteURL, file.DefaultDirOsMode, true)
	}
	if cfg.DependencyURL == "" {
		cfg.DependencyURL = url.Join(baseURL, "dependencies")
		_ = fs.Create(context.Background(), cfg.DependencyURL, file.DefaultDirOsMode, true)
	}

	cfg.Meta.Init()
	setter.SetStringIfEmpty(&cfg.APIPrefix, c.repository.APIPrefix)
	if !strings.HasPrefix(cfg.Meta.MetricURI, c.repository.APIPrefix) {
		cfg.Meta.MetricURI = strings.Replace(cfg.Meta.MetricURI, cfg.APIPrefix, c.repository.APIPrefix, 1)
	}
	if !strings.HasPrefix(cfg.Meta.StatusURI, c.repository.APIPrefix) {
		cfg.Meta.StatusURI = strings.Replace(cfg.Meta.StatusURI, cfg.APIPrefix, c.repository.APIPrefix, 1)
	}
	if !strings.HasPrefix(cfg.Meta.ConfigURI, c.repository.APIPrefix) {
		cfg.Meta.ConfigURI = strings.Replace(cfg.Meta.ConfigURI, cfg.APIPrefix, c.repository.APIPrefix, 1)
	}
	if !strings.HasPrefix(cfg.Meta.ViewURI, c.repository.APIPrefix) {
		cfg.Meta.ViewURI = strings.Replace(cfg.Meta.ViewURI, cfg.APIPrefix, c.repository.APIPrefix, 1)
	}
	if !strings.HasPrefix(cfg.Meta.OpenApiURI, c.repository.APIPrefix) {
		cfg.Meta.OpenApiURI = strings.Replace(cfg.Meta.OpenApiURI, cfg.APIPrefix, c.repository.APIPrefix, 1)
	}
	if !strings.HasPrefix(cfg.Meta.CacheWarmURI, c.repository.APIPrefix) {
		cfg.Meta.CacheWarmURI = strings.Replace(cfg.Meta.CacheWarmURI, cfg.APIPrefix, c.repository.APIPrefix, 1)
	}
	if !strings.HasPrefix(cfg.Meta.StructURI, c.repository.APIPrefix) {
		cfg.Meta.StructURI = strings.Replace(cfg.Meta.StructURI, cfg.APIPrefix, c.repository.APIPrefix, 1)
	}
	return nil
}

func (c *Config) loadConfig(ctx context.Context) error {
	var configs []interface{}
	for _, URL := range c.repository.Configs.URLs() {
		config, err := standalone.NewConfigFromURL(ctx, URL)
		if err != nil {
			return err
		}
		configs = append(configs, config)
	}
	merged, err := parser.MergeStructs(configs...)
	if err != nil {
		return err
	}
	c.Config = &standalone.Config{}
	return json.Unmarshal(merged, c.Config)
}

func (c *Config) inMemoryConfig() *standalone.Config {
	revealMetrics := true
	setter.SetIntIfNil(&c.repository.Port, 8080)
	return &standalone.Config{
		Config: &gateway.Config{
			ExposableConfig: gateway.ExposableConfig{
				APIPrefix:    c.repository.APIPrefix,
				RevealMetric: &revealMetrics,
			},
			SensitiveConfig: gateway.SensitiveConfig{
				APIKeys: dpath.APIKeys{
					{
						URI:    path.Join(c.repository.APIPrefix, "dev", "secured"),
						Header: "App-Secret-Id",
						Value:  "changeme",
					},
				},
			},
		},
		Endpoint: endpoint.Config{Port: *c.repository.Port},
	}
}

func (c *Config) NormalizeURL(repositoryURL string) {
	baseURL := url.Join(repositoryURL, "Datly")
	cfg := c.Config

	if url.IsRelative(cfg.RouteURL) {
		cfg.RouteURL = url.Join(baseURL, cfg.RouteURL)
	}
	if url.IsRelative(cfg.PluginsURL) {
		cfg.RouteURL = url.Join(baseURL, cfg.PluginsURL)
	}
	if url.IsRelative(cfg.DependencyURL) {
		cfg.RouteURL = url.Join(baseURL, cfg.DependencyURL)
	}
	cfg.URL = url.Join(baseURL, "config.json")
}

func NewConfig(repository *options.Repository) *Config {
	return &Config{
		repository: repository,
	}
}
