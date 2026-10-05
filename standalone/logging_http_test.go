package standalone

import (
	"context"
	"encoding/json"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/viant/datly/observability"
	"github.com/viant/datly/standalone/config"
	fixture "github.com/viant/datly/standalone/testdata/app"
	"github.com/viant/datly/standalone/testdata/app/records"
)

func TestStandaloneLoggingAcrossReloadAndEarlyHTTP(t *testing.T) {
	// No parallel test mutates stdout: the built-in legacy destination is frozen
	// at owner creation and remains the same across application generations.
	output, err := os.Create(filepath.Join(t.TempDir(), "logging.txt"))
	if err != nil {
		t.Fatal(err)
	}
	prior := os.Stdout
	os.Stdout = output
	defer func() { os.Stdout = prior; output.Close() }()
	f := fixture.New(t)
	cfg, err := (config.Loader{}).Load(context.Background(), f.Config)
	if err != nil {
		t.Fatal(err)
	}
	enabled := true
	cfg.Logging = &observability.Logging{EnableAudit: &enabled, EnableTracing: &enabled}
	exports, err := records.Exports()
	if err != nil {
		t.Fatal(err)
	}
	server, err := New(context.Background(), Options{Config: cfg, Registry: exports})
	if err != nil {
		t.Fatal(err)
	}
	defer server.Shutdown(context.Background())
	enabled = false // owner policy must be detached from caller mutation
	requests := []struct {
		method, path string
		status       int
	}{{"GET", "/records/1", 200}, {"GET", "/missing", 404}, {"POST", "/records/1", 405}}
	admission := func() {
		response := httptest.NewRecorder()
		server.manager.ServeHTTP(response, httptest.NewRequest("GET", "/unavailable", nil))
		if response.Code != 503 {
			t.Fatal("expected admission failure")
		}
	}
	admission()
	for revision := 1; revision <= 2; revision++ {
		if err := server.Reload(context.Background(), uint64(revision)); err != nil {
			t.Fatal(err)
		}
		for _, tc := range requests {
			response := httptest.NewRecorder()
			server.manager.ServeHTTP(response, httptest.NewRequest(tc.method, tc.path, nil))
			if response.Code != tc.status {
				t.Fatalf("status %d for %s %s", response.Code, tc.method, tc.path)
			}
		}
	}
	if err := server.Shutdown(context.Background()); err != nil {
		t.Fatal(err)
	}
	admission()
	if err := output.Sync(); err != nil {
		t.Fatal(err)
	}
	content, err := os.ReadFile(output.Name())
	if err != nil {
		t.Fatal(err)
	}
	audit, trace := 0, 0
	for _, line := range strings.Split(string(content), "\n") {
		if strings.HasPrefix(line, "[AUDIT] ") {
			var record struct {
				Status  int    `json:"statusCode"`
				URI     string `json:"uri"`
				Elapsed int    `json:"elapsedMs"`
			}
			if err := json.Unmarshal([]byte(strings.TrimPrefix(line, "[AUDIT] ")), &record); err != nil {
				t.Fatal(err)
			}
			want := requests[0]
			if audit == 0 || audit == 7 {
				want.path = "/unavailable"
				want.status = 503
			} else {
				want = requests[(audit-1)%3]
			}
			if record.Status != want.status || record.URI != want.path || record.Elapsed < 0 {
				t.Fatal("completion differs from transport")
			}
			audit++
		}
		if strings.HasPrefix(line, "[TRACE] ") {
			trace++
		}
	}
	if audit != 8 || trace != 8 {
		t.Fatalf("audit=%d trace=%d want8", audit, trace)
	}
}
