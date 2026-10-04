package observability

import (
	internallog "github.com/viant/datly/internal/logging"
	"os"
)

// Logging selects the original audit/trace profile. Nil preserves native v1
// behavior. A configured empty profile enables audit, disables trace and hides SQL.
type Logging struct {
	EnableAudit   *bool `json:"EnableAudit,omitempty" yaml:"EnableAudit,omitempty"`
	EnableTracing *bool `json:"EnableTracing,omitempty" yaml:"EnableTracing,omitempty"`
	IncludeSQL    *bool `json:"IncludeSQL,omitempty" yaml:"IncludeSQL,omitempty"`
}

// Copy detaches the startup policy from caller-owned optional values.
func (c *Logging) Copy() *Logging {
	if c == nil {
		return nil
	}
	result := *c
	if c.EnableAudit != nil {
		v := *c.EnableAudit
		result.EnableAudit = &v
	}
	if c.EnableTracing != nil {
		v := *c.EnableTracing
		result.EnableTracing = &v
	}
	if c.IncludeSQL != nil {
		v := *c.IncludeSQL
		result.IncludeSQL = &v
	}
	return &result
}

// WithLogging resolves the optional built-in policy once for the application
// recorder. Original audit/trace records use stdout independently of summaries.
func WithLogging(config *Logging) RecorderOption {
	profile := config.Copy()
	return func(recorder *Recorder) {
		if profile == nil {
			return
		}
		audit, trace, sql := true, false, false
		if profile.EnableAudit != nil {
			audit = *profile.EnableAudit
		}
		if profile.EnableTracing != nil {
			trace = *profile.EnableTracing
		}
		if profile.IncludeSQL != nil {
			sql = *profile.IncludeSQL
		}
		recorder.loggingState = internallog.NewState(internallog.NewSink(os.Stdout, audit, trace, sql))
	}
}
