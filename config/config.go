package config

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/pkg/errors"
	"github.com/viant/afs"
	"github.com/viant/afs/cache"
	"os"
	"strings"
)

//Config represents a config
type Config struct {
	URL          string
	CacheRules   bool `json:",omitempty"`
	Rules        Rules
	Connectors   Connectors
	DataCacheURL string `json:",omitempty"`
}

//Init initialises config
func (c *Config) Init(ctx context.Context, fs afs.Service) error {
	err := c.Rules.Init(ctx, fs)
	if err == nil {
		err = c.Connectors.Init(ctx, fs)
	}
	return err
}

//Validate checks if config is valid
func (c Config) Validate() error {
	if c.Rules.URL == "" {
		return errors.Errorf("rules.url was empty")
	}
	if c.Connectors.URL == "" {
		return errors.Errorf("connectors.url was empty")
	}
	if c.Rules.URL == c.Connectors.URL {
		return errors.Errorf("connectors and rule URL can not be the same: %v", c.Rules.URL)
	}
	return nil
}

//ReloadChanged reload changes if needed
func (c *Config) ReloadChanged(ctx context.Context, fs afs.Service) error {
	_, err := c.Rules.Loader.Notify(ctx, fs)
	if err == nil {
		_, err = c.Connectors.Loader.Notify(ctx, fs)
	}
	return err
}

//NewConfigFromEnv creates config from env
func NewConfigFromEnv(ctx context.Context, key string) (*Config, error) {
	if key == "" {
		return nil, errors.New("os env cfg key was empty")
	}
	data := os.Getenv(key)
	if data == "" {
		return nil, fmt.Errorf("env.%v was empty", key)
	}
	cfg := &Config{}
	err := json.NewDecoder(strings.NewReader(data)).Decode(cfg)
	if err != nil {
		return nil, errors.Wrapf(err, "failed to decode config :%s", data)
	}

	if err = cfg.Init(ctx, afs.New()); err != nil {
		return nil, err
	}
	err = cfg.Validate()
	return cfg, err
}

//NewConfigFromURL creates new config from URL
func NewConfigFromURL(ctx context.Context, URL string) (*Config, error) {
	if URL == "" {
		return nil, errors.Errorf("url was empty")
	}
	fs := afs.New()
	cfs := fs
	exists, err := fs.Exists(ctx, URL)
	if !exists {
		return nil, errors.Wrapf(err, "not found: %v", URL)
	}
	reader, err := fs.OpenURL(ctx, URL)
	if err != nil {
		return nil, errors.Wrapf(err, "failed to download config: %v", URL)
	}
	cfg := &Config{}
	err = json.NewDecoder(reader).Decode(cfg)
	if err != nil {
		return nil, errors.Wrapf(err, "failed to decode config :%s", URL)
	}
	cfg.URL = URL
	if cfg.CacheRules {
		cfs = cache.Singleton(URL)
	}
	if err = cfg.Init(ctx, cfs); err != nil {
		return cfg, err
	}
	err = cfg.Validate()
	return cfg, err
}

//NewConfig creates a new config from env (json or URL)
func NewConfig(ctx context.Context, source string) (*Config, error) {
	if source == "" {
		return nil, fmt.Errorf("config key was empty")
	}
	value := os.Getenv(source)
	if json.Valid([]byte(value)) {
		return NewConfigFromEnv(ctx, source)
	}
	if value == "" {
		value = source
	}
	return NewConfigFromURL(ctx, value)
}
