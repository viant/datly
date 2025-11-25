package logging

import (
	"encoding/json"
	"fmt"
	"github.com/viant/xdatly/handler/exec"
	"strconv"
	"time"
)

func Log(config *Config, execContext *exec.Context) {
	execContext.ElapsedMs = int(time.Since(execContext.StartTime).Milliseconds())
	includeSQL := config.ShallIncludeSQL()
	if !includeSQL {
		execContext.Metrics = execContext.Metrics.HideMetrics()
	}
	if config.IsAuditEnabled() {
		data := safeMarshal("EXECCONTEXT", execContext)
		fmt.Println("[AUDIT]", string(data))
	}
	if config.IsTracingEnabled() {
		trace := execContext.Trace
		rootSpan := trace.Spans[0]
		spans := execContext.Metrics.ToSpans(&rootSpan.SpanID)
		if execContext.Auth != nil {
			if execContext.Auth.UserID != 0 {
				rootSpan.Attributes["jwt.uid"] = strconv.Itoa(execContext.Auth.UserID)
			}
			if execContext.Auth.Username != "" {
				rootSpan.Attributes["jwt.username"] = execContext.Auth.Username
			}
			if execContext.Auth.Email != "" {
				rootSpan.Attributes["jwt.email"] = execContext.Auth.Email
			}
			if execContext.Auth.Scope != "" {
				rootSpan.Attributes["jwt.scope"] = execContext.Auth.Scope
			}
		}
		trace.Append(spans...)
		if execContext.Error != "" {
			trace.Spans[0].SetStatus(fmt.Errorf(execContext.Error))
		} else {
			trace.Spans[0].SetStatusFromHTTPCode(execContext.StatusCode)
		}
		traceData := safeMarshal("TRACE", trace)
		fmt.Println("[TRACE]", string(traceData))
	}
}

func safeMarshal(label string, v any) []byte {
	defer func() {
		if r := recover(); r != nil {
			fmt.Printf("[LOG-MARSHAL-PANIC] label=%s type=%T panic=%v\n", label, v, r)
		}
	}()
	data, err := json.Marshal(v)
	if err != nil {
		fmt.Printf("[LOG-MARSHAL-ERROR] label=%s type=%T err=%v\n", label, v, err)
		return nil
	}
	return data
}
