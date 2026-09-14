package observability

import "github.com/viant/xdatly/response"

// Read emits a summary only through an explicitly configured logger. Native
// metrics and counters are captured independently of logging configuration.
func (r *Recorder) Read(traceID string, m *response.Metric) {
	if r == nil || r.logger == nil || m == nil {
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
