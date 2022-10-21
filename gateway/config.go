package gateway

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/viant/afs"
	"github.com/viant/datly/auth/cognito"
	"github.com/viant/datly/auth/secret"
	"github.com/viant/datly/gateway/runtime/meta"
	"github.com/viant/datly/router"
	"github.com/viant/scy/auth/jwt/verifier"
	"github.com/viant/toolbox"
	"gopkg.in/yaml.v3"
	"sort"
	"strings"
	"time"
)

type (
	Config struct {
		APIPrefix            string //like /v1/api/
		RouteURL             string
		DependencyURL        string
		UseCacheFS           bool
		SyncFrequencyMs      int
		Secrets              []*secret.Resource
		JWTValidator         *verifier.Config
		Cognito              *cognito.Config
		Meta                 meta.Config
		APIKeys              router.APIKeys
		AutoDiscovery        *bool
		ChangeDetection      *ChangeDetection
		DisableCors          bool
		RevealMetricEnabled  *bool
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

func (c *Config) Init() {
	if c.SyncFrequencyMs == 0 {
		c.SyncFrequencyMs = 5000
	}

	if c.ChangeDetection == nil {
		c.ChangeDetection = &ChangeDetection{}
	}

	c.Meta.Init()
	c.ChangeDetection.Init()
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
	sort.Sort(cfg.APIKeys)
	return cfg, cfg.Validate()
}
