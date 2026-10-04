package logging

import (
	"context"
	"encoding/json"
	"fmt"
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
