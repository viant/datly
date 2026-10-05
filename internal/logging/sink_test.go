package logging

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"sync"

	"github.com/viant/xdatly/exec"
	"github.com/viant/xdatly/response"
	"github.com/viant/xdatly/tracing"
	"os"
	"reflect"
	"strings"
	"testing"
	"time"
)

func TestSinkPinnedLoggingGoldens(t *testing.T) {
	yes, no := true, false
	fixed := time.Date(2026, 1, 2, 3, 4, 5, 0, time.UTC)
	profiles := []struct {
		name string
		c    testProfile
	}{
		{"default", testProfile{}},
		{"audit_off_tracing_on", testProfile{EnableAudit: &no, EnableTracing: &yes}},
		{"both_plus_sql", testProfile{EnableAudit: &yes, EnableTracing: &yes, IncludeSQL: &yes}},
		{"explicitly_disabled", testProfile{EnableAudit: &no, EnableTracing: &no, IncludeSQL: &no}},
	}
	result := map[string][]map[string]interface{}{}
	for _, p := range profiles {
		c := &exec.Context{Method: "GET", URI: "/synthetic/items", StatusCode: 200, Status: "ok", StartTime: fixed, TraceID: "synthetic-trace", Header: map[string]string{"X-Request-Id": "synthetic-request"}, Metrics: response.Metrics{&response.Metric{ID: "synthetic-metric", View: "synthetic_items", Type: "SELECT", StartTime: fixed, EndTime: fixed.Add(2 * time.Millisecond), Elapsed: "2ms", ElapsedMs: 2, Rows: 1, Executions: response.SQLExecutions{&response.SQLExecution{ID: "synthetic-sql", StartTime: fixed, EndTime: fixed.Add(time.Millisecond), SQL: "SELECT id FROM synthetic_items WHERE id = ?", Args: []interface{}{42}, Rows: 1}}}}, Trace: &tracing.Trace{TraceID: "synthetic-trace", Resource: &tracing.ResourceInfo{ServiceName: "datly", ServiceVersion: "synthetic-version"}, Spans: []*tracing.Span{&tracing.Span{SpanID: "synthetic-root", Name: "HTTP GET /synthetic/items", Kind: "SERVER", StartTime: fixed, EndTime: fixed.Add(3 * time.Millisecond), Attributes: map[string]string{"http.method": "GET", "http.url": "/synthetic/items"}, Status: tracing.SpanStatus{Code: tracing.StatusOK}}}}}
		var output bytes.Buffer
		audit, trace, sql := true, false, false
		if p.c.EnableAudit != nil {
			audit = *p.c.EnableAudit
		}
		if p.c.EnableTracing != nil {
			trace = *p.c.EnableTracing
		}
		if p.c.IncludeSQL != nil {
			sql = *p.c.IncludeSQL
		}
		ctx := WithIdentityObservation(context.Background())
		ObserveIdentity(ctx, Identity{42, "synthetic-user", "synthetic@example.invalid", "synthetic:read"})
		NewSink(&output, audit, trace, sql).HTTP(ctx, c)
		b := output.Bytes()
		rows := []map[string]interface{}{}
		for _, line := range strings.Split(strings.TrimSpace(string(b)), "\n") {
			if line == "" {
				continue
			}
			parts := strings.SplitN(line, " ", 2)
			var value map[string]interface{}
			if e := json.Unmarshal([]byte(parts[1]), &value); e != nil {
				t.Fatal(e)
			}
			// SnapshotForLogging uses time.Since; normalize ONLY this wall-clock-derived field.
			if parts[0] == "[AUDIT]" {
				if _, ok := value["elapsedMs"]; !ok {
					t.Fatal("missing elapsedMs")
				}
				value["elapsedMs"] = "<wall-clock-derived>"
				auth := value["auth"].(map[string]interface{})
				if len(auth) != 4 || auth["user_id"] != float64(42) || auth["username"] != "synthetic-user" || auth["email"] != "synthetic@example.invalid" || auth["scope"] != "synthetic:read" {
					t.Fatalf("unexpected auth: %v", auth)
				}
			}
			rows = append(rows, map[string]interface{}{"prefix": parts[0], "payload": value})
		}
		result[p.name] = rows
		if c.Metrics[0].Executions[0].SQL == "" || len(c.Trace.Spans) != 1 || len(c.Trace.Spans[0].Attributes) != 2 {
			t.Fatal("live context mutated")
		}
	}
	if len(result["default"]) != 1 || len(result["audit_off_tracing_on"]) != 1 || len(result["both_plus_sql"]) != 2 || len(result["explicitly_disabled"]) != 0 {
		t.Fatal("unexpected emission counts")
	}
	b, e := os.ReadFile("testdata/original-logging.json")
	if e != nil {
		t.Fatal(e)
	}
	var want map[string][]map[string]interface{}
	if e = json.Unmarshal(b, &want); e != nil {
		t.Fatal(e)
	}
	if !reflect.DeepEqual(want, result) {
		actual, _ := json.MarshalIndent(result, "", "  ")
		t.Fatalf("source logging differs: %s", actual)
	}
}

type panicValue struct{}

func (panicValue) MarshalJSON() ([]byte, error) { panic("private-marshal-detail") }

type panicWriter struct{}

func (panicWriter) Write([]byte) (int, error) { panic("private-writer-detail") }

func TestSinkFailureDoesNotEscape(t *testing.T) {
	for _, writer := range []io.Writer{&bytes.Buffer{}, panicWriter{}} {
		c := exec.New()
		c.Metrics = response.Metrics{&response.Metric{Executions: response.SQLExecutions{&response.SQLExecution{Args: []any{panicValue{}}}}}}
		sink := NewSink(writer, true, true, true)
		func() {
			defer func() {
				if recovered := recover(); recovered != "application panic" {
					t.Fatalf("application panic replaced: %v", recovered)
				}
			}()
			defer sink.HTTP(context.Background(), c)
			panic("application panic")
		}()
		if buf, ok := writer.(*bytes.Buffer); ok && (strings.Contains(buf.String(), "private-") || !strings.Contains(buf.String(), "[LOG-MARSHAL-PANIC]")) {
			t.Fatal("unsafe or missing diagnostic")
		}
	}
}

func TestSinkConcurrentIdentityIsolation(t *testing.T) {
	var output bytes.Buffer
	sink := NewSink(&output, true, false, false)
	var wg sync.WaitGroup
	for i := 1; i <= 100; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			ctx := WithIdentityObservation(context.Background())
			ObserveIdentity(ctx, Identity{UserID: id})
			sink.HTTP(ctx, exec.New())
		}(i)
	}
	wg.Wait()
	seen := map[int]bool{}
	for _, line := range strings.Split(strings.TrimSpace(output.String()), "\n") {
		var record struct {
			Auth struct {
				UserID int `json:"user_id"`
			} `json:"auth"`
		}
		if err := json.Unmarshal([]byte(strings.TrimPrefix(line, "[AUDIT] ")), &record); err != nil {
			t.Fatal(err)
		}
		if seen[record.Auth.UserID] {
			t.Fatal("duplicate identity")
		}
		seen[record.Auth.UserID] = true
	}
	if len(seen) != 100 {
		t.Fatal("missing records")
	}
}

type testProfile struct{ EnableAudit, EnableTracing, IncludeSQL *bool }
