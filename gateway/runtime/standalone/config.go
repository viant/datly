package standalone

import (
	"context"
	"encoding/json"
	"github.com/viant/afs"
	"github.com/viant/afs/url"
	"github.com/viant/datly/gateway"
	"github.com/viant/datly/gateway/router/openapi/openapi3"
	"github.com/viant/datly/gateway/runtime/standalone/endpoint"
	"github.com/viant/toolbox"
	"gopkg.in/yaml.v3"
	"path/filepath"
	"strings"
)

type (
	//Config defines standalone app config
	Config struct {
		URL     string
		Version string
		*gateway.Config
		Endpoint endpoint.Config
		Info     openapi3.Info
	}
)

// init initialises config
func (c *Config) Init(ctx context.Context) {
	c.Config.Init(ctx)
	c.Endpoint.Init()
	if c.Cognito != nil {
		c.Cognito.Init()
	}

	//if c.PluginsURL == "" {
	//	baseURL, _ := url.Split(c.DependencyURL, file.Scheme)
	//	c.PluginsURL = url.Join(baseURL, "plugins")
	//}
}

// Validate validates config
func (c *Config) Validate() error {
	return nil
}

func NewConfigFromURL(ctx context.Context, URL string) (*Config, error) {
	fs := afs.New()
	data, err := fs.DownloadWithURL(ctx, URL)
	if err != nil {
		return nil, err
	}
	aMap := map[string]interface{}{}
	if strings.HasSuffix(URL, "yaml") {
		transient := map[string]interface{}{}
		if err := yaml.Unmarshal(data, &transient); err != nil {
			return nil, err
		}
		aMap = map[string]interface{}{}
		if err := yaml.Unmarshal(data, &aMap); err != nil {
			return nil, err
		}
	} else {
		aMap = map[string]interface{}{}
		if err := json.Unmarshal(data, &aMap); err != nil {
			return nil, err
		}
	}
	cfg := &Config{}
	err = toolbox.DefaultConverter.AssignConverted(cfg, aMap)
	if err != nil {
		return nil, err
	}
	cfg.URL = URL
	cfg.Init(ctx)
	cfg.normalizeURLs(baseDir(URL))
	return cfg, cfg.Validate()
}

func (c *Config) normalizeURLs(baseURL string) {
	if url.IsRelative(c.RouteURL) {
		c.RouteURL = url.Join(baseURL, c.RouteURL)
	}
	if url.IsRelative(c.ContentURL) {
		c.ContentURL = url.Join(baseURL, c.ContentURL)
	}
	if url.IsRelative(c.PluginsURL) {
		c.PluginsURL = url.Join(baseURL, c.PluginsURL)
	}
	if url.IsRelative(c.DependencyURL) {
		c.DependencyURL = url.Join(baseURL, c.DependencyURL)
	}
	if url.IsRelative(c.JobURL) {
		c.JobURL = url.Join(baseURL, c.JobURL)
	}
	if url.IsRelative(c.FailedJobURL) {
		c.FailedJobURL = url.Join(baseURL, c.FailedJobURL)
	}
}

func baseDir(URL string) string {
	if strings.Contains(URL, "://") {
		parent, _ := url.Split(URL, "file")
		return parent
	}
	return filepath.Dir(URL)
}
