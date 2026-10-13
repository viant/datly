package logging

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/viant/xdatly/response"
	"io"
	"strconv"
	"sync"

	xexec "github.com/viant/xdatly/exec"
)

// Sink serializes compatibility records synchronously. Its owner is shared by
// application generations; no per-request queues or goroutines are introduced.
type Sink struct {
	mu                sync.Mutex
	output            io.Writer
	audit, trace, sql bool
}

func NewSink(output io.Writer, audit, trace, sql bool) *Sink {
	return &Sink{output: output, audit: audit, trace: trace, sql: sql}
}

func (s *Sink) Enabled() bool { return s != nil && (s.audit || s.trace) }

// HTTP consumes a completed request snapshot without retaining mutable evidence.
// Logging failures cannot replace a transport result or an application panic.
func (s *Sink) HTTP(ctx context.Context, execution *xexec.Context) {
	if !s.Enabled() || execution == nil {
		return
	}
	defer func() {
		if recover() != nil {
			s.diagnostic("[LOG-MARSHAL-PANIC]")
		}
	}()
	snapshot, trace := xexec.PrepareLogging(execution, s.sql)
	identity, verified, conflict := IdentitySnapshot(ctx)
	if verified && trace != nil && len(trace.Spans) > 0 && trace.Spans[0] != nil {
		attributes := trace.Spans[0].Attributes
		if attributes == nil {
			attributes = map[string]string{}
			trace.Spans[0].Attributes = attributes
		}
		if identity.UserID != 0 {
			attributes["jwt.uid"] = strconv.Itoa(identity.UserID)
		}
		if identity.Username != "" {
			attributes["jwt.username"] = identity.Username
		}
		if identity.Email != "" {
			attributes["jwt.email"] = identity.Email
		}
		if identity.Scope != "" {
			attributes["jwt.scope"] = identity.Scope
		}
	}
	var audit any = snapshot
	if verified {
		audit = struct {
			*xexec.Context
			Auth *auditIdentity `json:"auth,omitempty"`
		}{snapshot, &auditIdentity{identity.UserID, identity.Username, identity.Email, identity.Scope}}
	}
	var records [][]byte
	if s.audit {
		data, err := json.Marshal(audit)
		if err != nil {
			s.diagnostic("[LOG-MARSHAL-ERROR]")
		} else {
			records = append(records, append(append([]byte("[AUDIT] "), redactSerializedJSON(data)...), '\n'))
		}
	}
	if s.trace {
		data, err := json.Marshal(trace)
		if err != nil {
			s.diagnostic("[LOG-MARSHAL-ERROR]")
		} else {
			records = append(records, append(append([]byte("[TRACE] "), redactSerializedJSON(data)...), '\n'))
		}
	}
	if conflict {
		records = append(records, []byte("[LOG-IDENTITY-CONFLICT]\n"))
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, record := range records {
		if s.output != nil {
			if _, err := s.output.Write(record); err != nil {
				return
			}
		}
	}
}

func (s *Sink) diagnostic(message string) {
	defer func() { _ = recover() }()
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.output != nil {
		_, _ = fmt.Fprintln(s.output, message)
	}
}

// These four names are verified against the pinned original audit payload.
type auditIdentity struct {
	UserID   int    `json:"user_id,omitempty"`
	Username string `json:"username,omitempty"`
	Email    string `json:"email,omitempty"`
	Scope    string `json:"scope,omitempty"`
}

// Read emits completion within the configured profile, including a redacted
// reason for preparation/read failures that may not have an SQLExecution.
func (s *Sink) Read(traceID string, metric *response.Metric) {
	if s == nil || metric == nil {
		return
	}
	if traceID == "" {
		traceID = "unknown"
	}
	status := "ok"
	if metric.Error != "" {
		status = "error"
	}
	context := ""
	if metric.ID != "" {
		context = " metricId=" + SafeText(metric.ID)
	}
	level, failure := "INFO", ""
	if metric.Error != "" {
		level = "ERROR"
		queries := make([]string, 0, len(metric.Executions))
		for _, e := range metric.Executions {
			if e != nil {
				queries = append(queries, e.SQL)
			}
		}
		failure = fmt.Sprintf(" error=%q", FailureReason(metric.Error, queries...))
	}
	elapsed := metric.Elapsed
	if elapsed == "" && !metric.StartTime.IsZero() && !metric.EndTime.IsZero() {
		elapsed = metric.EndTime.Sub(metric.StartTime).String()
	}
	s.diagnostic(fmt.Sprintf("[%s] datly view read reqTraceId=%s view=%s rows=%d elapsed=%s%s status=%s%s", level, SafeText(traceID), SafeText(metric.View), metric.Rows, SafeText(elapsed), context, status, failure))
}
