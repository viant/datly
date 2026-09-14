// Package otel exports completed native Datly records in one application-owned
// worker. No SDK span construction or exporter call occurs in TrySubmit.
package otel

import (
	"context"
	xexec "github.com/viant/xdatly/exec"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"time"
)

type Config struct {
	Enabled                     bool
	MaxSpans                    int
	BatchSize                   int
	BatchTimeout                time.Duration
	QueueSize                   int
	ExportTimeout               time.Duration
	ServiceName, ServiceVersion string
	// IncludeSQL is opt-in because authored SQL can itself contain sensitive literals.
	// Arguments, request headers/bodies and raw error messages are never exported.
	IncludeSQL bool
	Exporter   sdktrace.SpanExporter
}

// Link is trusted producer trace context, e.g. persisted async submit context.
// It does not infer linkage from DATLY_JOBS IDs, SQL, credentials or request data.
type Link struct{ TraceID, SpanID string }
type Completion struct {
	Context *xexec.Context
	End     time.Time
	Failed  bool
	Links   []Link
}
type linksKey struct{}

// WithLinks adds authoritative cross-invocation links, not a parent transaction
// or ambient request. Callers must obtain IDs from trusted execution metadata.
func WithLinks(ctx context.Context, links ...Link) context.Context {
	return context.WithValue(ctx, linksKey{}, append([]Link(nil), links...))
}
func Links(ctx context.Context) []Link { links, _ := ctx.Value(linksKey{}).([]Link); return links }

type Stats struct{ Accepted, Dropped, Exported, Failed uint64 }
