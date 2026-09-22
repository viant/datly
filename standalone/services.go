package standalone

import (
	"context"
	"io"
	"log/slog"
	"net/http"
	"os"
	"time"

	"github.com/viant/datly/exec"
	gateway "github.com/viant/datly/gateway/http"
	"github.com/viant/datly/observability/otel"
	"github.com/viant/datly/runtime"
)

func serviceLogger(diagnostics io.Writer) *slog.Logger {
	if diagnostics == nil {
		diagnostics = os.Stderr
	}
	return slog.New(slog.NewJSONHandler(diagnostics, nil))
}

func (s *source) services(ctx context.Context, logger *slog.Logger) (runtime.ObservabilityConfig, error) {
	s.http = s.config.Config
	if policy := s.config.Warmup; policy != nil {
		admin := *policy.Admin
		s.http.Warmup = &gateway.WarmupConfig{Timeout: time.Duration(policy.TimeoutMs) * time.Millisecond, AdminHeaders: []string{admin.APIKeyHeader}, Authorize: func(ctx context.Context, request *http.Request, _ exec.ComponentTarget) error {
			if err := ctx.Err(); err != nil {
				return err
			}
			return admin.Authorize(request)
		}, Completed: func(result gateway.WarmupResult, err error) {
			logger.Info("datly cache warmup completed", "target", result.Target, "status", result.Status, "groups", result.Groups, "failed", err != nil)
		}}
	}
	if policy := s.config.CacheInvalidation; policy != nil {
		admin := *policy.Admin
		s.http.CacheInvalidation = &gateway.CacheInvalidationConfig{Timeout: time.Duration(policy.TimeoutMs) * time.Millisecond, Authorize: func(ctx context.Context, request *http.Request, _ exec.ComponentTarget) error {
			if err := ctx.Err(); err != nil {
				return err
			}
			return admin.Authorize(request)
		}}
	}
	result := runtime.ObservabilityConfig{}
	if s.config.Observation == nil {
		return result, nil
	}
	if s.config.Observation.LogSummaries {
		result.Logger = logger
	}
	if configured := s.config.Observation.OTel; configured != nil && configured.Enabled {
		exporter, err := configured.HTTP.New(ctx, time.Duration(configured.ExportTimeoutMs)*time.Millisecond)
		if err != nil {
			return result, err
		}
		result.OTel = &otel.Config{Enabled: true, QueueSize: configured.QueueSize, BatchSize: configured.BatchSize, MaxSpans: configured.MaxSpans, BatchTimeout: time.Duration(configured.BatchTimeoutMs) * time.Millisecond, ExportTimeout: time.Duration(configured.ExportTimeoutMs) * time.Millisecond, ServiceName: configured.ServiceName, ServiceVersion: configured.ServiceVersion, IncludeSQL: configured.IncludeSQL, Exporter: exporter}
	}
	return result, nil
}
