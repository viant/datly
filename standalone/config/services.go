package config

import (
	"fmt"
	"net/url"
	"path/filepath"
	"strings"
	"time"

	gateway "github.com/viant/datly/gateway/http"
	"github.com/viant/datly/observability/otel"
)

type Warmup struct {
	TimeoutMs int64
	Admin     *gateway.DocumentAccess
}

type CacheInvalidation struct {
	TimeoutMs int64
	Admin     *gateway.DocumentAccess
}

// Observation configures the existing native capture owner. Logging summaries
// is independent of OTel; neither option disables native Datly capture.
type Observation struct {
	LogSummaries bool
	OTel         *OTel
}

type OTel struct {
	Enabled                         bool
	QueueSize, BatchSize, MaxSpans  int
	BatchTimeoutMs, ExportTimeoutMs int64
	ServiceName, ServiceVersion     string
	IncludeSQL                      bool
	HTTP                            otel.HTTPExporter
}

func (c *Config) validateServices() error {
	const maxMs = int64((1<<63 - 1) / int64(time.Millisecond))
	if w := c.Warmup; w != nil {
		if c.Config.Warmup != nil {
			return fmt.Errorf("configured and supplied Warmup policies conflict")
		}
		if w.TimeoutMs <= 0 || w.TimeoutMs > maxMs || w.Admin == nil {
			return fmt.Errorf("Warmup requires a positive TimeoutMs and Admin policy")
		}
		if err := w.Admin.Validate(); err != nil {
			return err
		}
		if strings.TrimSpace(c.Meta.CacheWarmURI) == "" && c.Meta.CacheWarmURI != "" {
			return fmt.Errorf("Warmup requires an enabled Meta.CacheWarmURI")
		}
	}
	if policy := c.CacheInvalidation; policy != nil {
		if c.Config.CacheInvalidation != nil {
			return fmt.Errorf("configured and supplied CacheInvalidation policies conflict")
		}
		if policy.TimeoutMs <= 0 || policy.TimeoutMs > maxMs || policy.Admin == nil {
			return fmt.Errorf("CacheInvalidation requires a positive TimeoutMs and Admin policy")
		}
		if err := policy.Admin.Validate(); err != nil {
			return err
		}
		if c.Meta.CacheInvalidateURI != "" && strings.TrimSpace(c.Meta.CacheInvalidateURI) == "" {
			return fmt.Errorf("CacheInvalidation requires an enabled Meta.CacheInvalidateURI")
		}
	}
	if c.Observation != nil && c.Observation.OTel != nil {
		o := c.Observation.OTel
		// Bound allocations from configuration before constructing the native queue.
		if o.QueueSize < 0 || o.QueueSize > 65536 || o.BatchSize < 0 || o.BatchSize > 65536 || o.MaxSpans < 0 || o.MaxSpans > 65536 || o.BatchTimeoutMs < 0 || o.BatchTimeoutMs > maxMs || o.ExportTimeoutMs < 0 || o.ExportTimeoutMs > maxMs {
			return fmt.Errorf("invalid OTel queue, span or timeout limits")
		}
		if o.Enabled {
			if err := o.HTTP.Validate(); err != nil {
				return err
			}
		}
	}
	if c.OpenAPI != nil {
		if err := c.OpenAPI.AggregateAccess.Validate(); err != nil {
			return err
		}
		if err := c.OpenAPI.RouteAccess.Validate(); err != nil {
			return err
		}
		seen := map[string]bool{}
		for _, export := range c.OpenAPI.StartupExports {
			parsed, err := url.Parse(export.URL)
			if err != nil || export.URL == "" || parsed.Scheme != "" && parsed.Scheme != "file" || parsed.Host != "" || parsed.RawQuery != "" || parsed.Fragment != "" {
				return fmt.Errorf("OpenAPI StartupExports requires local file destinations")
			}
			destination, err := filepath.Abs(parsed.Path)
			if err != nil {
				return fmt.Errorf("invalid OpenAPI export destination")
			}
			if seen[destination] {
				return fmt.Errorf("duplicate OpenAPI export destination")
			}
			seen[destination] = true
			if export.Format != "json" && export.Format != "yaml" {
				return fmt.Errorf("OpenAPI export Format must be json or yaml")
			}
			if strings.TrimSpace(c.Meta.OpenApiURI) == "" && c.Meta.OpenApiURI != "" {
				return fmt.Errorf("OpenAPI exports require publication")
			}
		}
	}
	return nil
}
