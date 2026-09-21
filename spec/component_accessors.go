package spec

import "strings"

func (c *Component) CacheWarmup() *CacheWarmupSettings {
	if c == nil || c.Settings == nil || c.Settings.Cache == nil {
		return nil
	}
	return c.Settings.Cache.Warmup
}

// CacheWarmups returns every effective warmup definition: singular first, then
// plural in declaration order, with case references expanded per warmup.
func (c *Component) CacheWarmups() ([]*CacheWarmupSettings, error) {
	if c == nil || c.Settings == nil || c.Settings.Cache == nil {
		return nil, nil
	}
	return c.Settings.Cache.EffectiveWarmups()
}

func (c *Component) RootSource() *ViewSource {
	if c == nil || c.RootView == nil {
		return nil
	}
	return c.RootView.Source
}

// RootSQL returns the trimmed root SQL text for presence checks and SQL-shape
// detection. Callers that need the raw authored SQL should use RootSource().
func (c *Component) RootSQL() string {
	source := c.RootSource()
	if source == nil {
		return ""
	}
	return strings.TrimSpace(source.SQL)
}
