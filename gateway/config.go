package gateway

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/viant/afs"
	"github.com/viant/datly/gateway/runtime/meta"
	"github.com/viant/datly/router"
	"github.com/viant/datly/service/auth/cognito"
	"github.com/viant/datly/service/auth/secret"
	"github.com/viant/scy/auth/jwt/signer"
	"github.com/viant/scy/auth/jwt/verifier"
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
		APIKeys router.APIKeys
	}

	ExposableConfig struct {
		APIPrefix            string //like /v1/api/
		RouteURL             string
		PluginsURL           string
		DependencyURL        string
		AssetsURL            string
		EnvURL               string `json:",omitempty" yaml:",omitempty"`
		UseCacheFS           bool
		SyncFrequencyMs      int
		Secrets              []*secret.Resource
		JWTValidator         *verifier.Config
		JwtSigner            *signer.Config
		Cognito              *cognito.Config
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

func (c *Config) Init() error {
	if c.SyncFrequencyMs == 0 {
		c.SyncFrequencyMs = 2000
	}
	if c.ChangeDetection == nil {
		c.ChangeDetection = &ChangeDetection{}
	}

	c.Meta.Init()
	c.ChangeDetection.Init()
	return c.APIKeys.Init()
}

func NewConfigFromURL(ctx context.Context, fs afs.Service, URL string) (*Config, error) {
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
	if err = cfg.Init(); err != nil {
		return nil, err
	}
	return cfg, cfg.Validate()
}
