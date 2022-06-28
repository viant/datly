package gateway

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/viant/afs"
	"github.com/viant/datly/auth/cognito"
	"github.com/viant/datly/auth/secret"
	"github.com/viant/datly/gateway/runtime/meta"
	"github.com/viant/toolbox"
	"gopkg.in/yaml.v3"
	"strings"
)

type Config struct {
	APIPrefix       string //like /v1/api/
	RouteURL        string
	DependencyURL   string
	UseCacheFS      bool
	SyncFrequencyMs int
	Secrets         []*secret.Resource
	Cognito         *cognito.Config
	Meta            meta.Config
	AutoDiscovery   *bool
}

func (c *Config) Validate() error {
	if c.RouteURL == "" {
		return fmt.Errorf("RouteURL was empty")
	}
	return nil
}

func (c *Config) Discovery() bool {
	return c.AutoDiscovery == nil || *c.AutoDiscovery
}

func (c *Config) Init() {
	if c.SyncFrequencyMs == 0 {
		c.SyncFrequencyMs = 5000
	}
	c.Meta.Init()
}

func NewConfigFromURL(ctx context.Context, URL string) (*Config, error) {
	fs := afs.New()
	data, err := fs.DownloadWithURL(ctx, URL)
	if err != nil {
		return nil, err
	}
	aMap := map[string]interface{}{}
	if strings.HasSuffix(URL, "yaml") {
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
	cfg.Init()
	return cfg, cfg.Validate()
}
