package gateway

import (
	"context"
	"fmt"
	"github.com/viant/afs"
	"github.com/viant/toolbox"
	"gopkg.in/yaml.v3"
)

type Config struct {
	APIPrefix       string //like /v1/api/
	BaseURL         string
	UseCacheFS      bool
	SyncFrequencyMs int
}

func (c *Config) Validate() error {
	if c.BaseURL == "" {
		return fmt.Errorf("BaseURL was empty")
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
	transient := map[string]interface{}{}
	if err := yaml.Unmarshal(data, &transient); err != nil {
		return nil, err
	}
	aMap := map[string]interface{}{}
	if err := yaml.Unmarshal(data, &aMap); err != nil {
		return nil, err
	}
	cfg := &Config{}
	err = toolbox.DefaultConverter.AssignConverted(cfg, aMap)
	if err != nil {
		return nil, err
	}
	cfg.Init()
	return cfg, cfg.Validate()
}
