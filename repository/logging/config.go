package logging

import "strings"

type Config struct {
	EnableTracing *bool
	EnableAudit   *bool
	IncludeSQL    *bool
	// AuditExcludeURIPrefixes skips [AUDIT] logging when the request URI has any
	// of these prefixes. Nil uses the default (meta introspect/metric scrapes).
	// Set to an empty slice to disable path exclusions.
	AuditExcludeURIPrefixes []string
}

var defaultAuditExcludeURIPrefixes = []string{"/v1/api/meta/"}

func (c *Config) IsTracingEnabled() bool {
	if c.EnableTracing == nil {
		return false
	}
	return *c.EnableTracing
}

func (c *Config) IsAuditEnabled() bool {
	if c.EnableAudit == nil {
		return true
	}
	return *c.EnableAudit
}

func (c *Config) ShallIncludeSQL() bool {
	if c.IncludeSQL == nil {
		return false
	}
	return *c.IncludeSQL
}

func (c *Config) auditExcludePrefixes() []string {
	if c == nil || c.AuditExcludeURIPrefixes == nil {
		return defaultAuditExcludeURIPrefixes
	}
	return c.AuditExcludeURIPrefixes
}

// ShouldAuditURI reports whether [AUDIT] should be emitted for the request URI.
func (c *Config) ShouldAuditURI(uri string) bool {
	if !c.IsAuditEnabled() {
		return false
	}
	path := uri
	if i := strings.IndexByte(uri, '?'); i >= 0 {
		path = uri[:i]
	}
	for _, prefix := range c.auditExcludePrefixes() {
		if prefix != "" && strings.HasPrefix(path, prefix) {
			return false
		}
	}
	return true
}
