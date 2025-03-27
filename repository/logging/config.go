package logging

type Config struct {
	EnableTracing *bool
	EnableAudit   *bool
	IncludeSQL    *bool
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
