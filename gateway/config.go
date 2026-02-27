package gateway

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/viant/afs"
	"github.com/viant/datly/gateway/runtime/meta"
	"github.com/viant/datly/repository/logging"
	"github.com/viant/datly/repository/path"
	"github.com/viant/datly/service/auth/config"
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
		DQLBootstrap    *DQLBootstrap
		ContentURL      string
		PluginsURL      string
		DependencyURL   string
		JobURL          string
		FailedJobURL    string
		MaxJobs         int
		UseCacheFS      bool
		SyncFrequencyMs int
		config.Config
		Logging              logging.Config
		Meta                 meta.Config
		AutoDiscovery        *bool
		ChangeDetection      *ChangeDetection
		DisableCors          bool
		CacheConnectorPrefix string
		Version              string
		CORS                 *path.Cors //Default CORS configuration
		MCP                  *ModelContextProtocol
	}

	ModelContextProtocol struct {
		Port              *int
		OAuth2ConfigURL   string
		IssuerURL         string
		AuthorizerMode    string
		BFFExchangeHeader string
		BFFRedirectURI    string
	}

	ChangeDetection struct {
		NumOfRetries     int
		RetryIntervalInS int
		_retry           time.Duration
	}

	DQLBootstrap struct {
		Sources             []string
		Exclude             []string
		FailFast            *bool
		Precedence          string
		CompileProfile      string
		MixedMode           string
		UnknownNonReadMode  string
		ColumnDiscoveryMode string
		DQLPathMarker       string
		RoutesRelativePath  string
	}
)

const (
	DQLBootstrapPrecedenceRoutesWins   = "routes_wins"
	DQLBootstrapPrecedenceDQLWins      = "dql_wins"
	DQLBootstrapPrecedenceErrorOnMixed = "error_on_conflict"
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
	if c.DQLBootstrap != nil && len(c.DQLBootstrap.Sources) == 0 {
		return fmt.Errorf("DQLBootstrap.Sources was empty")
	}
	if c.RouteURL == "" && !c.hasDQLBootstrap() {
		return fmt.Errorf("RouteURL was empty")
	}
	return nil
}

func (c *Config) hasDQLBootstrap() bool {
	return c != nil && c.DQLBootstrap != nil && len(c.DQLBootstrap.Sources) > 0
}

func (d *DQLBootstrap) ShouldFailFast() bool {
	if d == nil || d.FailFast == nil {
		return true
	}
	return *d.FailFast
}

func (d *DQLBootstrap) EffectivePrecedence() string {
	if d == nil {
		return DQLBootstrapPrecedenceRoutesWins
	}
	switch strings.TrimSpace(strings.ToLower(d.Precedence)) {
	case DQLBootstrapPrecedenceRoutesWins, DQLBootstrapPrecedenceDQLWins, DQLBootstrapPrecedenceErrorOnMixed:
		return strings.TrimSpace(strings.ToLower(d.Precedence))
	default:
		return DQLBootstrapPrecedenceRoutesWins
	}
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
	if c.CORS == nil {
		c.CORS = path.DefaultCors()
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
