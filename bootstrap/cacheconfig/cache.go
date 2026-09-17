// Package cacheconfig creates native SQLX cache services from authored settings.
package cacheconfig

import (
	"crypto/sha256"
	"fmt"
	"strings"
	"time"

	"github.com/viant/datly/spec"
	"github.com/viant/sqlx/io/read/cache"
	"github.com/viant/sqlx/io/read/cache/aerospike"
	"github.com/viant/sqlx/io/read/cache/afs"
)

// Config identifies one prepared view's native cache namespace. Identity must
// include the component and the view path, so unrelated connectors and views
// cannot share entries solely because their SQL text happens to match.
type Config struct {
	Settings *spec.CacheSettings
	Identity string
	// Aerospike is owned by the caller and closed after all readers and warmups drain.
	Aerospike *aerospike.Pool
}

type Validation struct {
	Location string
	Provider string
	TTL      time.Duration
}

// Validate checks settings without constructing a service or opening a connection.
func Validate(settings *spec.CacheSettings) error {
	_, err := (Config{Settings: settings}).validateSettings()
	return err
}

// Validate checks cache configuration without creating a provider, opening a
// pool, touching storage or performing network I/O.
func (c Config) Validate() (*Validation, error) {
	validated, err := c.validateSettings()
	if err != nil {
		return nil, err
	}
	if strings.TrimSpace(c.Identity) == "" {
		return nil, fmt.Errorf("cache view identity is required")
	}
	return validated, nil
}

func (c Config) validateSettings() (*Validation, error) {
	if c.Settings == nil {
		return nil, fmt.Errorf("cache settings are required")
	}
	if !c.Settings.Enabled {
		return nil, fmt.Errorf("cache %q is disabled", c.Settings.Name)
	}
	location := strings.TrimSpace(c.Settings.Location)
	if location == "" {
		return nil, fmt.Errorf("cache %q location is required", c.Settings.Name)
	}
	if c.Settings.TimeToLiveMs < 0 || c.Settings.TimeToLiveMs > int((1<<63-1)/int64(time.Millisecond)) {
		return nil, fmt.Errorf("cache %q timeToLiveMs is invalid", c.Settings.Name)
	}
	ttl := time.Duration(c.Settings.TimeToLiveMs) * time.Millisecond
	if c.Settings.TTL != "" {
		parsed, err := time.ParseDuration(c.Settings.TTL)
		if err != nil || parsed <= 0 {
			return nil, fmt.Errorf("cache %q TTL must be a positive duration", c.Settings.Name)
		}
		if ttl != 0 && ttl != parsed {
			return nil, fmt.Errorf("cache %q TTL and timeToLiveMs disagree", c.Settings.Name)
		}
		ttl = parsed
	}
	if ttl <= 0 {
		return nil, fmt.Errorf("cache %q TTL is required", c.Settings.Name)
	}
	provider := strings.TrimSpace(c.Settings.Provider)
	if !strings.HasPrefix(provider, "aerospike:") && provider != "" && !strings.EqualFold(provider, "afs") {
		return nil, fmt.Errorf("cache %q provider is not supported; supply a native cache service", c.Settings.Name)
	}
	return &Validation{Location: location, Provider: provider, TTL: ttl}, nil
}

func (c Config) New() (cache.Cache, error) {
	validated, err := c.Validate()
	if err != nil {
		return nil, err
	}
	location, provider, ttl := validated.Location, validated.Provider, validated.TTL
	if strings.HasPrefix(provider, "aerospike:") {
		service, err := c.Aerospike.NewCache(aerospike.Config{
			Provider: provider, Location: location, Identity: c.Identity, TTL: ttl,
			Timeout: aerospike.TimeoutConfig{
				MaxRetries: c.Settings.MaxRetries, TotalTimeoutMs: c.Settings.TotalTimeoutInMs,
				SocketTimeoutMs: c.Settings.SocketTimeoutInMs, SleepBetweenRetriesMs: c.Settings.SleepBetweenRetriesInMs,
			},
			FailedRequestLimit: c.Settings.FailedRequestLimit, ResetFailuresInMs: c.Settings.ResetFailuresInMs,
		})
		if err != nil {
			return nil, err
		}
		return service, nil
	}
	switch strings.ToLower(provider) {
	case "", "afs":
		namespace := fmt.Sprintf("%x", sha256.Sum256([]byte(c.Identity)))
		return afs.NewCache(strings.TrimRight(location, "/")+"/"+namespace, ttl, c.Identity, nil)
	default:
		return nil, fmt.Errorf("cache provider %q is not supported; supply a native cache service", c.Settings.Provider)
	}
}
