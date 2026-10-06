package reader_test

import (
	"context"
	"errors"
	"io"
	"os"
	"strings"
	"sync"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/viant/datly/internal/testharness/sqlite"
	"github.com/viant/datly/observability"
	"github.com/viant/datly/observability/otel"
	"github.com/viant/datly/runtime"
	xexec "github.com/viant/xdatly/exec"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
)

type policyExportProbe struct {
	mu    sync.Mutex
	spans []sdktrace.ReadOnlySpan
}

func (p *policyExportProbe) ExportSpans(_ context.Context, spans []sdktrace.ReadOnlySpan) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.spans = append(p.spans, spans...)
	return errors.New("private exporter failure")
}
func (*policyExportProbe) Shutdown(context.Context) error { return nil }

type policySummaryProbe struct {
	mu        sync.Mutex
	summaries int
}

func (*policySummaryProbe) Debug(string, ...any) {}
func (*policySummaryProbe) Warn(string, ...any)  {}
func (*policySummaryProbe) Error(string, ...any) {}
func (p *policySummaryProbe) Info(message string, _ ...any) {
	if message == "datly view read" {
		p.mu.Lock()
		p.summaries++
		p.mu.Unlock()
	}
}

func TestObservationMappedExportFailureAndCompatibilityCompletionSQLite(t *testing.T) {
	read, write, err := os.Pipe()
	require.NoError(t, err)
	previous := os.Stdout
	os.Stdout = write
	defer func() { os.Stdout = previous; read.Close(); write.Close() }()
	ctx := context.Background()
	db := sqlite.New(t)
	require.NoError(t, db.ExecStatements(ctx, "CREATE TABLE records(id INTEGER)", "INSERT INTO records VALUES(7)"))
	export := &policyExportProbe{}
	logger := &policySummaryProbe{}
	app, a := policyLifecycleRuntime(t, db.DB, runtime.ObservabilityConfig{Logging: &observability.Logging{}, Logger: logger, OTel: &otel.Config{Enabled: true, Exporter: export, QueueSize: 16}})
	for _, trace := range []string{"mapped-export-one", "mapped-export-two"} {
		ec := xexec.New()
		ec.TraceID = trace
		require.NoError(t, policyInvoke(xexec.WithContext(ctx, ec), app, a))
		require.Len(t, ec.Metrics, 1)
		require.Equal(t, "records#", ec.Metrics[0].View)
	}
	require.NoError(t, app.Observability().Shutdown(ctx))
	values := app.Observability().Recorder.Values("platform.records")
	require.Len(t, values, 11)
	require.EqualValues(t, 2, values["Success"])
	require.Zero(t, values["Pending"])
	require.Zero(t, values["Error"])
	require.EqualValues(t, 2, app.Observability().Recorder.Cumulative("platform.records", "count"))
	require.Empty(t, app.Observability().Recorder.Values(a.Component.Key.String()+"/records"))
	require.Positive(t, app.Observability().ExportStats().Failed)
	export.mu.Lock()
	defer export.mu.Unlock()
	views := 0
	for _, span := range export.spans {
		for _, attribute := range span.Attributes() {
			if string(attribute.Key) == "datly.view" {
				require.Equal(t, "records#", attribute.Value.AsString())
				views++
			}
			require.NotEqual(t, "db.statement", string(attribute.Key))
			require.NotEqual(t, "db.query.text", string(attribute.Key))
			require.NotContains(t, attribute.Value.AsString(), "private exporter failure")
		}
	}
	require.GreaterOrEqual(t, views, 2)
	write.Close()
	data, err := io.ReadAll(read)
	require.NoError(t, err)
	lines := strings.Split(strings.TrimSpace(string(data)), "\n")
	require.Len(t, lines, 2)
	for _, line := range lines {
		require.Contains(t, line, "[INFO] datly view read reqTraceId=mapped-export-")
		require.Contains(t, line, " view=records# rows=1 elapsed=")
		require.True(t, strings.HasSuffix(line, " status=ok"))
		require.NotContains(t, line, "SELECT")
		require.NotContains(t, line, "private")
	}
	logger.mu.Lock()
	defer logger.mu.Unlock()
	require.Zero(t, logger.summaries, "existing compatibility sink must prevent duplicate native summaries")
}
