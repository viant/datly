package observability

import (
	internallog "github.com/viant/datly/internal/logging"
	"github.com/viant/xdatly/response"
)

// Read emits one completion through configured compatibility logging or the
// native logger. Metrics and counters are independent of logging configuration.
func (r *Recorder) Read(traceID string, m *response.Metric) {
	if r == nil || m == nil {
		return
	}
	if internallog.LogRead(r, traceID, m) || r.logger == nil {
		return
	}
	status := "ok"
	if m.Error != "" {
		status = "error"
	}
	if traceID == "" {
		traceID = "unknown"
	}
	r.logger.Info("datly view read", "reqTraceId", traceID, "view", m.View, "rows", m.Rows, "elapsed", m.Elapsed, "status", status)
}

func (r *Recorder) SQL(traceID, view string, e *response.SQLExecution) {
	if r == nil || r.logger == nil || e == nil {
		return
	}
	if traceID == "" {
		traceID = "unknown"
	}
	if e.CacheStats != nil {
		s := e.CacheStats
		r.logger.Info("datly cache read", "reqTraceId", traceID, "view", view, "type", s.Type, "rows", e.Rows, "found_warmup", s.FoundWarmup, "found_lazy", s.FoundLazy)
	}
	if e.Error != "" {
		r.logger.Error("datly SQL read failed", "reqTraceId", traceID, "view", view)
	}
}
