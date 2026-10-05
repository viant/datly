package http

import (
	"context"
	"encoding/json"
	"github.com/viant/datly/observability"
	druntime "github.com/viant/datly/runtime"
	"github.com/viant/datly/spec"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"testing/fstest"
)

func TestEnabledLoggingStaticDocumentAdminBranches(t *testing.T) {
	output, err := os.Create(filepath.Join(t.TempDir(), "logging.txt"))
	if err != nil {
		t.Fatal(err)
	}
	prior := os.Stdout
	os.Stdout = output
	defer func() { os.Stdout = prior; output.Close() }()
	enabled := true
	f := newConfigFixture(t)
	rt := f.runtime(t, druntime.WithObservability(druntime.ObservabilityConfig{Logging: &observability.Logging{EnableAudit: &enabled, EnableTracing: &enabled}}))
	defer rt.Shutdown(context.Background())
	cfg := cacheConfig()
	h, err := cfg.NewHandler(rt, nil, "test")
	if err != nil {
		t.Fatal(err)
	}
	// Stage frozen UI bytes directly: this fixture's reader is not a canonical
	// Build registration and therefore cannot stage an OpenAPI schema.
	h.documents = &documentRoutes{prefix: "/docs/openapi", uiPath: "/docs/ui", aggregate: &documentRoute{access: DocumentAccess{APIKeyHeader: "X-Doc", APIKeyValue: "allowed"}}, ui: []byte("<html>docs</html>")}
	h.static = []*staticRoute{{prefix: "/assets", content: &spec.StaticContent{Path: "/assets"}, handler: http.FileServerFS(fstest.MapFS{"hello.txt": {Data: []byte("hello")}})}}
	requests := []struct {
		method, path, body, key string
		status                  int
	}{
		{"GET", "/assets/hello.txt", "", "", 200}, {"HEAD", "/assets/hello.txt", "", "", 200}, {"GET", "/assets/missing", "", "", 404},
		{"GET", "/docs/openapi", "", "", 403}, {"GET", "/docs/openapi/missing", "", "", 404}, {"GET", "/docs/ui", "", "allowed", 200},
		{"POST", "/admin/cache/records", `{"scope":"lazy"}`, "", 403}, {"POST", "/admin/cache/records", `{"scope":"lazy"}`, "admin", 200},
	}
	for _, tc := range requests {
		r := httptest.NewRequest(tc.method, tc.path, strings.NewReader(tc.body))
		r.Header.Set("X-Doc", tc.key)
		r.Header.Set("X-Admin", tc.key)
		w := httptest.NewRecorder()
		h.ServeHTTP(w, r)
		if w.Code != tc.status {
			t.Fatalf("%s %s status=%d body=%s", tc.method, tc.path, w.Code, w.Body.String())
		}
	}
	if err := output.Sync(); err != nil {
		t.Fatal(err)
	}
	data, err := os.ReadFile(output.Name())
	if err != nil {
		t.Fatal(err)
	}
	audit, trace := 0, 0
	for _, line := range strings.Split(string(data), "\n") {
		if strings.HasPrefix(line, "[AUDIT] ") {
			var record struct {
				URI    string `json:"uri"`
				Status int    `json:"statusCode"`
				Method string `json:"method"`
			}
			if err := json.Unmarshal([]byte(strings.TrimPrefix(line, "[AUDIT] ")), &record); err != nil {
				t.Fatal(err)
			}
			if audit >= len(requests) {
				t.Fatal("duplicate audit")
			}
			tc := requests[audit]
			if record.URI != tc.path || record.Status != tc.status {
				t.Fatalf("audit=%+v expected=%+v", record, tc)
			}
			audit++
		}
		if strings.HasPrefix(line, "[TRACE] ") {
			trace++
		}
	}
	if audit != len(requests) || trace != len(requests) {
		t.Fatalf("audit=%d trace=%d want=%d", audit, trace, len(requests))
	}
}
