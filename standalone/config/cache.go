package config

import (
	"fmt"
	"reflect"
	"sort"
	"strings"

	"github.com/viant/datly/bootstrap/cacheconfig"
	"github.com/viant/datly/spec"
)

// CacheProvider is the legacy CacheProviders list entry. Legacy definitions
// predate Enabled, so an omitted flag enables the provider; explicit false wins.
type CacheProvider struct {
	spec.CacheSettings
	Enabled *bool `json:"enabled,omitempty"`
}

// namedCaches returns detached settings and never overrides a conflicting name.
func namedCaches(caches map[string]*spec.CacheSettings, providers []*CacheProvider) (map[string]*spec.CacheSettings, error) {
	result := make(map[string]*spec.CacheSettings, len(caches)+len(providers))
	add := func(name string, settings *spec.CacheSettings) error {
		if name == "" || strings.TrimSpace(name) != name {
			return fmt.Errorf("cache name must be nonempty without surrounding whitespace")
		}
		if settings == nil {
			return fmt.Errorf("cache %q settings are required", name)
		}
		if settings.Name != "" && settings.Name != name {
			return fmt.Errorf("cache %q name disagrees with its map key", name)
		}
		item := settings.Clone()
		item.Name = name
		if previous, ok := result[name]; ok && !reflect.DeepEqual(previous, item) {
			return fmt.Errorf("conflicting definitions for cache %q", name)
		}
		result[name] = item
		return nil
	}
	for _, name := range cacheNames(caches) {
		if err := add(name, caches[name]); err != nil {
			return nil, err
		}
	}
	for _, provider := range providers {
		if provider == nil {
			return nil, fmt.Errorf("cache provider settings are required")
		}
		settings := provider.CacheSettings
		settings.Enabled = provider.Enabled == nil || *provider.Enabled
		if err := add(settings.Name, &settings); err != nil {
			return nil, err
		}
	}
	return result, nil
}

func cacheNames(caches map[string]*spec.CacheSettings) []string {
	names := make([]string, 0, len(caches))
	for name := range caches {
		names = append(names, name)
	}
	sort.Strings(names)
	return names
}

func (c *Config) validateCaches() error {
	caches, err := namedCaches(c.Caches, c.CacheProviders)
	if err != nil {
		return err
	}
	for _, name := range cacheNames(caches) {
		settings := caches[name]
		if !settings.Enabled {
			continue
		}
		if err := cacheconfig.Validate(settings); err != nil {
			return err
		}
	}
	return nil
}
