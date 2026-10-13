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
	elapsed := m.Elapsed
	if elapsed == "" && !m.StartTime.IsZero() && !m.EndTime.IsZero() {
		elapsed = m.EndTime.Sub(m.StartTime).String()
	}
	args := []any{"reqTraceId", internallog.SafeText(traceID), "view", internallog.SafeText(m.View), "rows", m.Rows, "elapsed", internallog.SafeText(elapsed), "status", status}
	if m.ID != "" {
		args = append(args, "metricId", internallog.SafeText(m.ID))
	}
	if m.Error != "" {
		queries := make([]string, 0, len(m.Executions))
		for _, e := range m.Executions {
			if e != nil {
				queries = append(queries, e.SQL)
			}
		}
		args = append(args, "error", internallog.FailureReason(m.Error, queries...))
		r.logger.Error("datly view read", args...)
		return
	}
	r.logger.Info("datly view read", args...)
}

func (r *Recorder) SQL(traceID, view string, e *response.SQLExecution) {
	if r == nil || r.logger == nil || e == nil {
		return
	}
	if traceID == "" {
		traceID = "unknown"
	}
	args := []any{"reqTraceId", internallog.SafeText(traceID), "view", internallog.SafeText(view), "rows", e.Rows}
	if e.ID != "" {
		args = append(args, "executionId", internallog.SafeText(e.ID))
	}
	if e.ParentID != "" {
		args = append(args, "parentId", internallog.SafeText(e.ParentID))
	}
	if !e.StartTime.IsZero() && !e.EndTime.IsZero() {
		args = append(args, "elapsed", e.EndTime.Sub(e.StartTime).String())
	}
	if s := e.CacheStats; s != nil {
		args = append(args, "type", internallog.SafeText(s.Type), "cache_type", internallog.SafeText(s.Type), "cache_namespace", internallog.SafeText(s.Namespace), "cache_set", internallog.SafeText(s.Dataset), "cache_records", s.RecordsCounter, "found_warmup", s.FoundWarmup, "found_lazy", s.FoundLazy, "cache_error_type", internallog.SafeText(s.ErrorType), "cache_error_code", s.ErrorCode)
		if s.ErrorType != "" || s.ErrorCode != 0 {
			r.logger.Warn("datly cache read", args...)
		} else {
			r.logger.Info("datly cache read", args...)
		}
	}
	if e.Error != "" {
		args = append(args, "error", internallog.FailureReason(e.Error, e.SQL))
		r.logger.Error("datly SQL read failed", args...)
	}
}
