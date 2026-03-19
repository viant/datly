package logging

import (
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"runtime/debug"
	"strconv"

	"github.com/viant/xdatly/handler/exec"
)

func Log(config *Config, execContext *exec.Context) {
	snap := execContext.SnapshotForLogging()
	includeSQL := config.ShallIncludeSQL()
	if !includeSQL {
		snap.Metrics = snap.Metrics.HideMetrics()
	}
	if config.IsAuditEnabled() {
		data := safeMarshal("EXECCONTEXT", snap)
		fmt.Println("[AUDIT]", string(data))
	}
	if config.IsTracingEnabled() {
		trace := snap.Trace
		rootSpan := trace.Spans[0]
		spans := snap.Metrics.ToSpans(&rootSpan.SpanID)
		if snap.Auth != nil {
			if snap.Auth.UserID != 0 {
				rootSpan.Attributes["jwt.uid"] = strconv.Itoa(snap.Auth.UserID)
			}
			if snap.Auth.Username != "" {
				rootSpan.Attributes["jwt.username"] = snap.Auth.Username
			}
			if snap.Auth.Email != "" {
				rootSpan.Attributes["jwt.email"] = snap.Auth.Email
			}
			if snap.Auth.Scope != "" {
				rootSpan.Attributes["jwt.scope"] = snap.Auth.Scope
			}
		}
		trace.Append(spans...)
		if snap.Error != "" {
			trace.Spans[0].SetStatus(errors.New(snap.Error))
		} else {
			trace.Spans[0].SetStatusFromHTTPCode(snap.StatusCode)
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
