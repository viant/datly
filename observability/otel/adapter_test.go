package otel

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	xexec "github.com/viant/xdatly/exec"
	"github.com/viant/xdatly/response"
	"github.com/viant/xdatly/tracing"
	"go.opentelemetry.io/otel/codes"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
)

type exportProbe struct {
	mu       sync.Mutex
	batches  [][]sdktrace.ReadOnlySpan
	gate     <-chan struct{}
	started  chan struct{}
	once     sync.Once
	fail     bool
	shutdown error
}

func (p *exportProbe) ExportSpans(ctx context.Context, spans []sdktrace.ReadOnlySpan) error {
	if p.started != nil {
		p.once.Do(func() { close(p.started) })
	}
	if p.gate != nil {
		select {
		case <-p.gate:
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	if p.fail {
		return errors.New("synthetic export error")
	}
	p.mu.Lock()
	p.batches = append(p.batches, append([]sdktrace.ReadOnlySpan(nil), spans...))
	p.mu.Unlock()
	return nil
}
func (p *exportProbe) Shutdown(context.Context) error { return p.shutdown }
func completedFixture() Completion {
	start := time.Date(2026, 9, 13, 12, 0, 0, 0, time.UTC)
	parent := "0102030405060708"
	expiry := start.Add(time.Hour)
	return Completion{End: start.Add(time.Second), Failed: true, Context: &xexec.Context{TraceID: "12345678-1234-5678-90ab-1234567890ab", StartTime: start, Header: map[string]string{"Authorization": "secret-JWT"}, Trace: &tracing.Trace{Spans: []*tracing.Span{{SpanID: "request-native", ParentSpanID: &parent, StartTime: start, Attributes: map[string]string{"http.url": "?secret=credentials"}}}}, Metrics: response.Metrics{{ID: "metric-native", View: "records", Type: "SELECT", StartTime: start.Add(time.Millisecond), EndTime: start.Add(5 * time.Millisecond), Rows: 2, Error: "sensitive error JWT", Executions: response.SQLExecutions{{ID: "root-sql-native", StartTime: start.Add(2 * time.Millisecond), EndTime: start.Add(3 * time.Millisecond), Rows: 2, SQL: "SELECT 'credentials'", Args: []any{"secret-JWT"}, CacheStats: &response.CacheStats{Key: "sensitive cache key", CreatedTime: &start, ExpiryTime: &expiry, FoundLazy: true, RecordsCounter: 2}}}}, {ID: "child-metric-native", View: "children", Type: "SELECT", StartTime: start.Add(6 * time.Millisecond), EndTime: start.Add(9 * time.Millisecond), Executions: response.SQLExecutions{{ID: "child-sql-native", ParentID: "root-sql-native", StartTime: start.Add(7 * time.Millisecond), EndTime: start.Add(8 * time.Millisecond), Error: "password=secret"}}}}}, Links: []Link{{TraceID: "11111111111111111111111111111111", SpanID: "2222222222222222"}}}
}
func TestMappingNativeTimingParentageAndPrivacy(t *testing.T) {
	exporter := &exportProbe{}
	a, err := New(Config{Enabled: true, Exporter: exporter, QueueSize: 2})
	if err != nil {
		t.Fatal(err)
	}
	c := completedFixture()
	start := c.Context.StartTime
	if !a.TrySubmit(c) {
		t.Fatal("rejected")
	}
	if err = a.Shutdown(context.Background()); err != nil {
		t.Fatal(err)
	}
	spans := exporter.batches[0]
	if len(spans) != 5 {
		t.Fatalf("spans=%d", len(spans))
	}
	by := map[string]sdktrace.ReadOnlySpan{}
	for _, span := range spans {
		by[span.Name()] = span
		if span.SpanContext().TraceID().String() != "123456781234567890ab1234567890ab" {
			t.Fatal("trace mapping")
		}
		for _, a := range span.Attributes() {
			value := a.Value.Emit()
			if strings.Contains(value, "secret") || strings.Contains(value, "credentials") || strings.Contains(value, "password") || strings.Contains(value, "JWT") {
				t.Fatalf("unsafe attr %v", a)
			}
		}
		for _, e := range span.Events() {
			t.Fatalf("unexpected unsafe event %v", e)
		}
	}
	root := by["datly request"]
	sql := by["SQL Select: records"]
	child := by["SQL Select: children"]
	createdFound := false
	for _, attr := range sql.Attributes() {
		if attr.Key == "cache.created_unix_nano" {
			createdFound = true
			if attr.Value.AsInt64() != start.UnixNano() {
				t.Fatal("cache creation timestamp changed")
			}
		}
	}
	if !createdFound {
		t.Fatal("cache creation timestamp missing")
	}
	if !root.StartTime().Equal(start) || !root.EndTime().Equal(c.End) || root.Parent().SpanID().String() != "0102030405060708" || len(root.Links()) != 1 {
		t.Fatal("root timing/parent/link lost")
	}
	if !sql.StartTime().Equal(start.Add(2*time.Millisecond)) || !sql.EndTime().Equal(start.Add(3*time.Millisecond)) {
		t.Fatal("SQL timestamps changed")
	}
	if child.Parent().SpanID() != sql.SpanContext().SpanID() || sql.Parent().SpanID() != by["Assemble: records"].SpanContext().SpanID() {
		t.Fatal("hierarchy lost")
	}
	if child.Status().Code != codes.Error || child.Status().Description != "operation failed" {
		t.Fatal("error status lost")
	}
	if c.Context.Metrics[0].Executions[0].Args[0] != "secret-JWT" {
		t.Fatal("native records changed")
	}
}
func TestSnapshotDetachedAndQueueDrop(t *testing.T) {
	gate := make(chan struct{})
	p := &exportProbe{gate: gate, started: make(chan struct{})}
	a, _ := New(Config{Enabled: true, Exporter: p, QueueSize: 2})
	c := completedFixture()
	if !a.TrySubmit(c) {
		t.Fatal("first rejected")
	}
	<-p.started
	if !a.TrySubmit(c) {
		t.Fatal("second rejected")
	}
	c.Context.Metrics[0].Rows = 999
	c.Context.Metrics[0].Executions[0].Rows = 999
	*c.Context.Trace.Spans[0].ParentSpanID = "ffffffffffffffff"
	*c.Context.Metrics[0].Executions[0].CacheStats.ExpiryTime = time.Time{}
	*c.Context.Metrics[0].Executions[0].CacheStats.CreatedTime = time.Time{}
	if a.TrySubmit(c) {
		t.Fatal("queue must drop while full")
	}
	if c.Context.Metrics[0].Rows != 999 {
		t.Fatal("native retention lost")
	}
	close(gate)
	if err := a.Shutdown(context.Background()); err != nil {
		t.Fatal(err)
	}
	if s := a.Stats(); s.Accepted != 2 || s.Exported != 2 || s.Dropped != 1 {
		t.Fatalf("stats=%+v", s)
	}
	for _, span := range p.batches[1] {
		for _, attr := range span.Attributes() {
			if attr.Key == "cache.created_unix_nano" && attr.Value.AsInt64() != c.Context.StartTime.UnixNano() {
				t.Fatal("queued creation timestamp retained mutable pointer")
			}
			if attr.Key == "datly.rows" && attr.Value.AsInt64() == 999 {
				t.Fatal("queued snapshot retained mutable metric")
			}
		}
		if span.Name() == "datly request" && span.Parent().SpanID().String() != "0102030405060708" {
			t.Fatal("queued parent pointer retained")
		}
	}
	if a.TrySubmit(c) {
		t.Fatal("accepted after shutdown")
	}
	if err := a.Shutdown(context.Background()); err != nil {
		t.Fatal(err)
	}
}
func TestConcurrentAdmissionShutdownAndExportErrors(t *testing.T) {
	for _, fail := range []bool{false, true} {
		t.Run(map[bool]string{false: "success", true: "export-error"}[fail], func(t *testing.T) {
			p := &exportProbe{fail: fail}
			a, _ := New(Config{Enabled: true, Exporter: p, QueueSize: 8})
			c := completedFixture()
			var wg sync.WaitGroup
			for i := 0; i < 8; i++ {
				wg.Add(1)
				go func() {
					defer wg.Done()
					for j := 0; j < 50; j++ {
						a.TrySubmit(c)
					}
				}()
			}
			shutdown := make(chan error, 1)
			go func() { shutdown <- a.Shutdown(context.Background()) }()
			wg.Wait()
			if err := <-shutdown; err != nil {
				t.Fatal(err)
			}
			if err := a.Shutdown(context.Background()); err != nil {
				t.Fatal(err)
			}
			s := a.Stats()
			if s.Accepted+s.Dropped != 400 || s.Accepted != s.Exported+s.Failed {
				t.Fatalf("unaccounted admission %+v", s)
			}
			if fail && s.Exported != 0 {
				t.Fatal("failed export counted successful")
			}
		})
	}
}
func TestShutdownTimeoutAndValidation(t *testing.T) {
	p := &exportProbe{gate: make(chan struct{}), started: make(chan struct{})}
	a, _ := New(Config{Enabled: true, Exporter: p, QueueSize: 1})
	a.TrySubmit(completedFixture())
	<-p.started
	ctx, cancel := context.WithTimeout(context.Background(), time.Millisecond)
	defer cancel()
	if err := a.Shutdown(ctx); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("shutdown=%v", err)
	}
	if err := a.Shutdown(context.Background()); err != nil {
		t.Fatal(err)
	}
	if a.Stats().Failed != 1 {
		t.Fatal("cancelled export not accounted")
	}
	if disabled, err := New(Config{}); err != nil || disabled != nil {
		t.Fatal("disabled adapter allocated")
	}
	if _, err := New(Config{Enabled: true}); err == nil {
		t.Fatal("missing exporter accepted")
	}
}
func TestInvalidNativeRecordsFailExportOnly(t *testing.T) {
	for _, kind := range []string{"duplicate", "interval", "link", "cycle"} {
		t.Run(kind, func(t *testing.T) {
			c := completedFixture()
			switch kind {
			case "duplicate":
				c.Context.Metrics[1].ID = c.Context.Metrics[0].ID
			case "interval":
				c.Context.Metrics[0].EndTime = c.Context.StartTime
			case "link":
				c.Links[0].SpanID = "job-123"
			case "cycle":
				c.Context.Metrics[0].Executions[0].ParentID = "child-sql-native"
			}
			p := &exportProbe{}
			a, _ := New(Config{Enabled: true, Exporter: p})
			if !a.TrySubmit(c) {
				t.Fatal("admission failed")
			}
			a.Shutdown(context.Background())
			if a.Stats().Failed != 1 || len(p.batches) != 0 {
				t.Fatal("invalid mapping exported")
			}
		})
	}
}

func TestExporterShutdownError(t *testing.T) {
	want := errors.New("shutdown failed")
	a, _ := New(Config{Enabled: true, Exporter: &exportProbe{shutdown: want}})
	if err := a.Shutdown(context.Background()); !errors.Is(err, want) {
		t.Fatal(err)
	}
}

func TestNativeSpanProjectionAgreementAndLimits(t *testing.T) {
	c := completedFixture()
	parent := "metric-native"
	c.Context.Trace.Spans = append(c.Context.Trace.Spans, &tracing.Span{SpanID: "native-custom", ParentSpanID: &parent, Name: "body=secret", Kind: "INTERNAL", StartTime: c.Context.StartTime, EndTime: c.End, Status: tracing.SpanStatus{Code: tracing.StatusUnset}})
	p := &exportProbe{}
	a, _ := New(Config{Enabled: true, Exporter: p})
	a.TrySubmit(c)
	a.Shutdown(context.Background())
	if a.Stats().Failed != 0 {
		t.Fatal("mapping failed")
	}
	rootID := c.Context.Trace.Spans[0].SpanID
	native := c.Context.Metrics.ToSpans(&rootID)
	mapped := map[string]sdktrace.ReadOnlySpan{}
	for _, span := range p.batches[0] {
		mapped[span.SpanContext().SpanID().String()] = span
	}
	ids := mappedIDs{}
	tid := ids.traceID(c.Context.TraceID)
	for _, old := range native {
		span := mapped[ids.spanID(tid, old.SpanID).String()]
		if span == nil || span.Name() != old.Name || !span.StartTime().Equal(old.StartTime) || !span.EndTime().Equal(old.EndTime) {
			t.Fatal("native ToSpans mapping differs")
		}
		if old.ParentSpanID != nil && span.Parent().SpanID() != ids.spanID(tid, *old.ParentSpanID) {
			t.Fatal("native parent projection differs")
		}
	}
	custom := mapped[ids.spanID(tid, "native-custom").String()]
	if custom.Status().Code != codes.Unset || custom.Name() != "datly span" {
		t.Fatal("native custom status or privacy lost")
	}
	bounded, _ := New(Config{Enabled: true, Exporter: &exportProbe{}, MaxSpans: 2})
	if bounded.TrySubmit(c) {
		t.Fatal("oversize record accepted")
	}
	bounded.Shutdown(context.Background())
}

func TestGeneratedRootsAreUniqueWithinIncomingTrace(t *testing.T) {
	p := &exportProbe{}
	a, _ := New(Config{Enabled: true, Exporter: p})
	c := completedFixture()
	c.Context.Trace = nil
	a.TrySubmit(c)
	a.TrySubmit(c)
	a.Shutdown(context.Background())
	var roots []sdktrace.ReadOnlySpan
	for _, batch := range p.batches {
		for _, span := range batch {
			if span.Name() == "datly request" {
				roots = append(roots, span)
			}
		}
	}
	if len(roots) != 2 || roots[0].SpanContext().SpanID() == roots[1].SpanContext().SpanID() {
		t.Fatal("synthesized root ID reused")
	}
}

func TestBatchingExportsWholeCompletedRecords(t *testing.T) {
	p := &exportProbe{}
	a, _ := New(Config{Enabled: true, Exporter: p, QueueSize: 8, BatchSize: 4, BatchTimeout: time.Second})
	for i := 0; i < 4; i++ {
		c := completedFixture()
		c.Context.TraceID = fmt.Sprintf("%032x", i+1)
		if !a.TrySubmit(c) {
			t.Fatal("unexpected drop")
		}
	}
	if err := a.Shutdown(context.Background()); err != nil {
		t.Fatal(err)
	}
	if len(p.batches) != 1 || len(p.batches[0]) != 20 || a.Stats().Exported != 4 {
		t.Fatal("completed records were not batched together")
	}
}
