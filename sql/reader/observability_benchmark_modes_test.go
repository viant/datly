package reader_test

import (
	"context"
	"github.com/viant/datly/observability/otel"
	"github.com/viant/datly/runtime"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"time"
)

var benchmarkModes = []string{"capture", "adapter-fast"}

type benchmarkExporter struct{ delay time.Duration }

func (e benchmarkExporter) ExportSpans(ctx context.Context, spans []sdktrace.ReadOnlySpan) error {
	if e.delay > 0 {
		select {
		case <-time.After(e.delay):
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	for _, span := range spans {
		_ = span.Attributes()
	}
	return nil
}
func (benchmarkExporter) Shutdown(context.Context) error { return nil }
func benchmarkFixture(mode string) *obsRewriteFixture {
	config := runtime.ObservabilityConfig{}
	if mode != "capture" {
		config.OTel = &otel.Config{Enabled: mode != "adapter-off", QueueSize: 256, Exporter: benchmarkExporter{}}
		if mode == "adapter-slow" {
			config.OTel.Exporter = benchmarkExporter{delay: 2 * time.Millisecond}
		}
	}
	return newObsRewriteFixture(runtime.WithObservability(config))
}
func benchmarkClose(f *obsRewriteFixture) adapterLatency {
	start := time.Now()
	if err := f.app.Observability().Shutdown(context.Background()); err != nil {
		panic(err)
	}
	stats := f.app.Observability().ExportStats()
	return adapterLatency{Accepted: stats.Accepted, Dropped: stats.Dropped, Exported: stats.Exported, Failed: stats.Failed, DrainNS: time.Since(start).Nanoseconds()}
}
