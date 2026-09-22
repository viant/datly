package otel

import (
	xexec "github.com/viant/xdatly/exec"
	"github.com/viant/xdatly/tracing"
)

// detached uses the native snapshot owner, then retains only exportable fields.
func (c Completion) detached(includeSQL bool) Completion {
	snapshot := c.Context.SnapshotForLogging()
	safe := &xexec.Context{StartTime: snapshot.StartTime, TraceID: snapshot.TraceID, Method: snapshot.Method, StatusCode: snapshot.StatusCode, Status: snapshot.Status, Metrics: snapshot.Metrics}
	if snapshot.Trace != nil {
		safe.Trace = &tracing.Trace{TraceID: snapshot.Trace.TraceID, Resource: snapshot.Trace.Resource}
		for index, source := range snapshot.Trace.Spans {
			if source == nil {
				safe.Trace.Spans = append(safe.Trace.Spans, nil)
				continue
			}
			span := *source
			if span.ParentSpanID != nil {
				parent := *span.ParentSpanID
				span.ParentSpanID = &parent
			}
			span.Attributes = nil
			span.Name = "datly span"
			if index == 0 {
				span.Name = "datly request"
			}
			span.Status.Message = ""
			safe.Trace.Spans = append(safe.Trace.Spans, &span)
		}
	}
	for _, m := range safe.Metrics {
		if m == nil {
			continue
		}
		if m.Error != "" {
			m.Error = "operation failed"
		}
		for _, e := range m.Executions {
			if e == nil {
				continue
			}
			e.Args = nil
			if !includeSQL {
				e.SQL = ""
			}
			if e.Error != "" {
				e.Error = "read failed"
			}
			if e.CacheStats != nil {
				e.CacheStats.SetCreatedTime(e.CacheStats.CreatedTime)
				if e.CacheStats.ExpiryTime != nil {
					expiry := *e.CacheStats.ExpiryTime
					e.CacheStats.ExpiryTime = &expiry
				}
				e.CacheStats.Key = ""
				e.CacheStats.Dataset = ""
				e.CacheStats.Namespace = ""
				if e.CacheStats.ErrorType != "" {
					e.CacheStats.ErrorType = "cache error"
				}
			}
		}
	}
	c.Failed = c.Failed || snapshot.Error != "" || snapshot.Status == "error"
	c.Context = safe
	c.Links = append([]Link(nil), c.Links...)
	return c
}
