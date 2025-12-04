package logging

import (
	"encoding/json"
	"fmt"
	"reflect"
	"runtime/debug"
	"strconv"
	"time"

	"github.com/viant/xdatly/handler/exec"
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
			fmt.Printf("[LOG-MARSHAL-PANIC] label=%s type=%T panic=%v\nSTACK:\n%s\n", label, v, r, debug.Stack())
			if execCtx, ok := v.(*exec.Context); ok {
				findBadField(execCtx)
			}
		}
	}()
	data, err := json.Marshal(v)
	if err != nil {
		fmt.Printf("[LOG-MARSHAL-ERROR] label=%s type=%T err=%v\n", label, v, err)
		return nil
	}
	return data
}

func findBadField(execCtx *exec.Context) {
	val := reflect.ValueOf(execCtx).Elem()
	typ := val.Type()
	for i := 0; i < val.NumField(); i++ {
		field := val.Field(i)
		fieldType := typ.Field(i)
		fieldName := fieldType.Name

		// Skip unexported fields
		if !field.CanInterface() {
			continue
		}

		func() {
			defer func() {
				if r := recover(); r != nil {
					fmt.Printf("[BAD-FIELD-PANIC] %s (%s): %v\n", fieldName, field.Type(), r)
				}
			}()
			if _, err := json.Marshal(field.Interface()); err != nil {
				fmt.Printf("[BAD-FIELD-ERROR] %s (%s): %v\n", fieldName, field.Type(), err)
			}
		}()
	}
}
