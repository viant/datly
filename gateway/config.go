package gateway

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/viant/afs"
	"github.com/viant/datly/gateway/runtime/meta"
	"github.com/viant/datly/repository/path"
	"github.com/viant/datly/service/auth"
	"github.com/viant/datly/service/auth/secret"
	"github.com/viant/toolbox"
	"gopkg.in/yaml.v3"
	"strings"
	"time"
)

type (
	Config struct {
		ExposableConfig
		SensitiveConfig `json:",omitempty" yaml:",omitempty"`
	}

	SensitiveConfig struct {
		APIKeys path.APIKeys
	}

	ExposableConfig struct {
		APIPrefix       string //like /v1/api/
		RouteURL        string
		ContentURL      string
		PluginsURL      string
		DependencyURL   string
		JobURL          string
		FailedJobURL    string
		MaxJobs         int
		UseCacheFS      bool
		SyncFrequencyMs int
		auth.Config
		Meta                 meta.Config
		AutoDiscovery        *bool
		ChangeDetection      *ChangeDetection
		DisableCors          bool
		RevealMetric         *bool
		CacheConnectorPrefix string
	}

	ChangeDetection struct {
		NumOfRetries     int
		RetryIntervalInS int
		_retry           time.Duration
	}
)

func (d *ChangeDetection) Init() {
	if d.NumOfRetries == 0 {
		d.NumOfRetries = 15
	}

	if d.RetryIntervalInS == 0 {
		d.RetryIntervalInS = 60
	}

	d._retry = time.Second * time.Duration(d.RetryIntervalInS)
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

func (c *Config) Init(ctx context.Context) error {
	if c.SyncFrequencyMs == 0 {
		c.SyncFrequencyMs = 2000
	}
	if c.ChangeDetection == nil {
		c.ChangeDetection = &ChangeDetection{}
	}

	c.Meta.Init()
	c.ChangeDetection.Init()
	if err := c.APIKeys.Init(ctx); err != nil {
		return err
	}

	return c.initSecrets(ctx)
}

func (c *Config) initSecrets(ctx context.Context) error {
	if len(c.Secrets) == 0 {
		return nil
	}
	secrets := secret.New()
	for _, sec := range c.Secrets {
		if err := secrets.Apply(ctx, sec); err != nil {
			return err
		}
	}
	return nil
}

func (c *Config) SyncFrequency() time.Duration {
	return time.Duration(c.SyncFrequencyMs) * time.Millisecond
}

func NewConfigFromURL(ctx context.Context, fs afs.Service, URL string) (*Config, error) {
	fs = NewFs(URL, fs)
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
	if err = cfg.Init(ctx); err != nil {
		return nil, err
	}
	return cfg, cfg.Validate()
}
