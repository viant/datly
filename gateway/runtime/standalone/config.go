package standalone

import (
	"context"
	"encoding/json"
	"github.com/viant/afs"
	"github.com/viant/datly/gateway"
	"github.com/viant/datly/gateway/router/openapi3"
	"github.com/viant/datly/gateway/runtime/standalone/endpoint"
	"github.com/viant/toolbox"
	"gopkg.in/yaml.v3"
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

//Init initialises config
func (c *Config) Init() {
	c.Config.Init()
	c.Endpoint.Init()
	if c.Cognito != nil {
		c.Cognito.Init()
	}

	//if c.PluginsURL == "" {
	//	baseURL, _ := url.Split(c.DependencyURL, file.Scheme)
	//	c.PluginsURL = url.Join(baseURL, "plugins")
	//}
}

//Validate validates config
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
	cfg.Init()
	return cfg, cfg.Validate()
}
