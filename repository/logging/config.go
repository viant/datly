package logging

import "strings"

type Config struct {
	EnableTracing *bool
	EnableAudit   *bool
	IncludeSQL    *bool
	// AuditExcludeURIPrefixes skips [AUDIT] logging when the request URI has any
	// of these prefixes. Empty/nil means no path-based exclusions (library default:
	// audit all URIs when audit is enabled). Clients configure this, e.g.
	// ["/v1/api/meta/"] for metric/status scrape noise.
	AuditExcludeURIPrefixes []string
}

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

// ShouldAuditURI reports whether [AUDIT] should be emitted for the request URI.
func (c *Config) ShouldAuditURI(uri string) bool {
	if !c.IsAuditEnabled() {
		return false
	}
	if c == nil || len(c.AuditExcludeURIPrefixes) == 0 {
		return true
	}
	path := uri
	if i := strings.IndexByte(uri, '?'); i >= 0 {
		path = uri[:i]
	}
	for _, prefix := range c.AuditExcludeURIPrefixes {
		if prefix != "" && strings.HasPrefix(path, prefix) {
			return false
		}
	}
	return true
}
