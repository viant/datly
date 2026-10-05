package standalone

import (
	"context"
	"encoding/json"
	"github.com/viant/datly/observability"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestEnabledLoggingAsyncAndShutdownDrain(t *testing.T) {
	output, err := os.Create(filepath.Join(t.TempDir(), "logging.txt"))
	if err != nil {
		t.Fatal(err)
	}
	prior := os.Stdout
	os.Stdout = output
	defer func() { os.Stdout = prior; output.Close() }()
	f := newStandaloneAsync(t)
	enabled := true
	f.cfg.Logging = &observability.Logging{EnableAudit: &enabled, EnableTracing: &enabled}
	entered, release := make(chan struct{}), make(chan struct{})
	defer func() {
		select {
		case <-release:
		default:
			close(release)
		}
	}()
	f.handlerGate = func(context.Context) error { close(entered); <-release; return nil }
	f.start(t)
	token := f.jwt.Sign(t, time.Now().Add(time.Hour))
	job := f.schedule(t, "logging-queued", token)
	if r := f.request("GET", "/async-status/"+job.ID, "", token); r.Code != 200 {
		t.Fatalf("inspect=%d", r.Code)
	}
	// A synchronous async route is held inside its accepted execution. Shutdown
	// must close admission and wait for transport completion before returning.
	response := httptest.NewRecorder()
	requestDone := make(chan struct{})
	go func() {
		defer close(requestDone)
		r := f.request("POST", "/async-records?key=logging-sync&wait=true", `{"data":{"id":3,"name":"sync"}}`, token)
		*response = *r
	}()
	select {
	case <-entered:
	case <-time.After(5 * time.Second):
		t.Fatal("request did not enter handler")
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := f.server.Shutdown(ctx); err != context.Canceled {
		t.Fatalf("shutdown error=%v", err)
	}
	rejected := httptest.NewRecorder()
	f.server.ServeHTTP(rejected, httptest.NewRequest("GET", "/drain-rejected", nil))
	if rejected.Code != 503 {
		t.Fatalf("admission status=%d", rejected.Code)
	}
	close(release)
	select {
	case <-requestDone:
	case <-time.After(5 * time.Second):
		t.Fatal("request did not finish")
	}
	if err := f.server.Shutdown(context.Background()); err != nil {
		t.Fatal(err)
	}
	if response.Code != 200 {
		t.Fatalf("accepted response=%d %s", response.Code, response.Body.String())
	}
	output.Sync()
	data, err := os.ReadFile(output.Name())
	if err != nil {
		t.Fatal(err)
	}
	audit, trace := 0, 0
	statuses := map[string]int{}
	for _, line := range strings.Split(string(data), "\n") {
		if strings.HasPrefix(line, "[AUDIT] ") {
			var record struct {
				URI    string `json:"uri"`
				Status int    `json:"statusCode"`
			}
			if err := json.Unmarshal([]byte(strings.TrimPrefix(line, "[AUDIT] ")), &record); err != nil {
				t.Fatal(err)
			}
			if _, exists := statuses[record.URI]; exists {
				t.Fatalf("duplicate completion for %s", record.URI)
			}
			statuses[record.URI] = record.Status
			audit++
		}
		if strings.HasPrefix(line, "[TRACE] ") {
			trace++
		}
	}
	if audit != 4 || trace != 4 {
		t.Fatalf("audit=%d trace=%d want4 statuses=%v", audit, trace, statuses)
	}
	for uri, status := range map[string]int{"/async-records?key=logging-queued": 200, "/async-status/" + job.ID: 200, "/async-records?key=logging-sync&wait=true": 200, "/drain-rejected": 503} {
		if statuses[uri] != status {
			t.Fatalf("%s audit status=%d want%d", uri, statuses[uri], status)
		}
	}
}
