package lambda

import (
	"context"
	"encoding/json"
	"github.com/viant/afs"
	"github.com/viant/datly/auth/cognito"
	"github.com/viant/datly/gateway"
	"github.com/viant/datly/gateway/runtime/standalone/meta"
	"github.com/viant/toolbox"
	"gopkg.in/yaml.v3"
	"strings"
)

type Config struct {
	gateway.Config
	Cognito *cognito.Config
	Meta    meta.Config
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
	cfg.Meta.Init()
	return cfg, cfg.Validate()
}
