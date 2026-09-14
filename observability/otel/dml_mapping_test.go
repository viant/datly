package otel

import (
	"context"
	"strings"
	"testing"
	"time"

	xexec "github.com/viant/xdatly/exec"
	"github.com/viant/xdatly/response"
)

func TestOriginalAnonymousDMLMapping(t *testing.T) {
	start := time.Now()
	native := &xexec.Context{StartTime: start, TraceID: "123456781234567890ab1234567890ab"}
	for _, operation := range []string{"INSERT", "UPDATE", "DELETE"} {
		native.AppendMetrics(&response.Metric{View: "records", Type: operation, Rows: 1, StartTime: start, EndTime: start.Add(time.Millisecond)})
	}
	native.Metrics[1].Error = "native operation failure"
	exporter := &exportProbe{}
	adapter, err := New(Config{Enabled: true, Exporter: exporter})
	if err != nil {
		t.Fatal(err)
	}
	if !adapter.TrySubmit(Completion{Context: native, End: start.Add(time.Second), Failed: true}) {
		t.Fatal("admission failed")
	}
	if err = adapter.Shutdown(context.Background()); err != nil {
		t.Fatal(err)
	}
	if adapter.Stats().Failed != 0 || len(exporter.batches) != 1 || len(exporter.batches[0]) != 4 {
		t.Fatalf("export stats=%+v batches=%d", adapter.Stats(), len(exporter.batches))
	}
	spans := exporter.batches[0]
	root := spans[0].SpanContext().SpanID()
	ids := map[string]bool{}
	for i, span := range spans[1:] {
		id := span.SpanContext().SpanID().String()
		if ids[id] || span.Parent().SpanID() != root {
			t.Fatal("DML identity or parentage lost")
		}
		ids[id] = true
		if span.StartTime() != native.Metrics[i].StartTime || span.EndTime() != native.Metrics[i].EndTime {
			t.Fatal("native DML interval changed")
		}
		generated := false
		for _, attr := range span.Attributes() {
			if string(attr.Key) == "datly.span.id.generated" {
				generated = attr.Value.AsBool()
			}
			if strings.Contains(string(attr.Key), "commit") || string(attr.Key) == "db.query.text" {
				t.Fatal("fabricated SQL or commit state")
			}
		}
		if !generated || native.Metrics[i].ID != "" || len(native.Metrics[i].Executions) != 0 {
			t.Fatal("native record modified or synthetic identity hidden")
		}
	}
	if native.Metrics[1].Error != "native operation failure" {
		t.Fatal("native error changed")
	}
}

func TestAnonymousReadStillFailsClosed(t *testing.T) {
	c := completedFixture()
	c.Context.Metrics[0].ID = ""
	p := &exportProbe{}
	a, err := New(Config{Enabled: true, Exporter: p})
	if err != nil {
		t.Fatal(err)
	}
	a.TrySubmit(c)
	a.Shutdown(context.Background())
	if a.Stats().Failed != 1 || len(p.batches) != 0 {
		t.Fatal("missing reader identity accepted")
	}
}
