package gateway

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/viant/afs"
	"github.com/viant/datly/auth/secret"
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
}

func (c *Config) Validate() error {
	if c.RouteURL == "" {
		return fmt.Errorf("RouteURL was empty")
	}
	return nil
}

func (c *Config) Init() {
	if c.SyncFrequencyMs == 0 {
		c.SyncFrequencyMs = 5000
	}
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
	cfg.Init()
	return cfg, cfg.Validate()
}
