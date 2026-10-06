package observability

import (
	"io"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/viant/datly/spec"
	"github.com/viant/xdatly/response"
)

// This reproducer records the original008258 gaps before production edits.
func TestMigrationBaselineNativeOwnershipAndSchema(t *testing.T) {
	r := NewRecorder(nil)
	name := "component:first:records/datafees"
	r.Pending(name, 1)
	r.Begin(name, time.Now())(time.Now(), "Success")
	r.Pending(name, -1)
	if values := r.Values(name); len(values) != 14 || values["Success"] != 1 || values["Pending"] != 0 {
		t.Fatalf("native schema or actual lifecycle changed: %v", values)
	}
	if len(r.Values("platform.delivery.advertiser.datafees")) != 0 {
		t.Fatal("unexpected source owner")
	}
	o := r.operations[name]
	if o.Location != "datly" || o.Description != "view performance" {
		t.Fatalf("native descriptor changed: %+v", o)
	}
}

func TestConfiguredLoggingEmitsOriginalCompletion(t *testing.T) {
	read, write, err := os.Pipe()
	if err != nil {
		t.Fatal(err)
	}
	previous := os.Stdout
	os.Stdout = write
	defer func() { os.Stdout = previous; read.Close(); write.Close() }()
	r := NewRecorder(nil, WithLogging(&Logging{}))
	r.Read("baseline-trace", &response.Metric{View: "datafees#", Rows: 1, Elapsed: "1ms"})
	write.Close()
	data, err := io.ReadAll(read)
	if err != nil {
		t.Fatal(err)
	}
	if string(data) != "[INFO] datly view read reqTraceId=baseline-trace view=datafees# rows=1 elapsed=1ms status=ok\n" {
		t.Fatalf("source completion missing: %s", data)
	}
	// The original008258 baseline reproducer is frozen outside this SDK.
}

func TestMigrationBaselineEffectiveViewIdentityIncludesNamespace(t *testing.T) {
	a := &spec.View{Name: "datafees", Namespace: "adf"}
	b := &spec.View{Name: "datafees", Namespace: "other"}
	x, err := a.Identity()
	if err != nil {
		t.Fatal(err)
	}
	y, err := b.Identity()
	if err != nil {
		t.Fatal(err)
	}
	if x == y || x != "view::datafees|namespace:adf" {
		t.Fatalf("effective identities %q %q", x, y)
	}
}

func TestConfiguredCompletionErrorCRLFAndRedaction(t *testing.T) {
	read, write, err := os.Pipe()
	if err != nil {
		t.Fatal(err)
	}
	previous := os.Stdout
	os.Stdout = write
	defer func() { os.Stdout = previous; read.Close(); write.Close() }()
	r := NewRecorder(nil, WithLogging(&Logging{}))
	r.Read("trace\r\ninjected", &response.Metric{View: "datafees#\r\nforged", Rows: 2, Elapsed: "1ms\r\nextra", Error: "private failure SQL SELECT secret FROM sensitive WHERE token='private-token'"})
	write.Close()
	data, err := io.ReadAll(read)
	if err != nil {
		t.Fatal(err)
	}
	want := "[INFO] datly view read reqTraceId=trace  injected view=datafees#  forged rows=2 elapsed=1ms  extra status=error\n"
	if string(data) != want {
		t.Fatalf("error completion/sanitization mismatch: %q", string(data))
	}
	for _, private := range []string{"SELECT", "private-token", "sensitive", "private failure"} {
		if strings.Contains(string(data), private) {
			t.Fatal("private failure leaked")
		}
	}
}
