package standalone

import (
	"context"
	"encoding/json"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"

	gateway "github.com/viant/datly/gateway/http"
	"github.com/viant/datly/internal/testharness"
	"github.com/viant/datly/spec"
	"github.com/viant/datly/standalone/config"
	fixture "github.com/viant/datly/standalone/testdata/app"
	"github.com/viant/datly/standalone/testdata/app/records"
)

func TestConfiguredServicesSourceSQLite(t *testing.T) {
	f := fixture.New(t)
	f.Services(t, "")
	cfg, err := (config.Loader{}).Load(context.Background(), f.Config)
	if err != nil {
		t.Fatal(err)
	}
	exports, err := records.Exports()
	if err != nil {
		t.Fatal(err)
	}
	var diagnostics testharness.Output
	s, err := New(context.Background(), Options{Config: cfg, Registry: exports, Diagnostics: &diagnostics})
	if err != nil {
		t.Fatal(err)
	}
	defer s.Shutdown(context.Background())
	var warmErr error
	completed := s.source.http.Warmup.Completed
	s.source.http.Warmup.Completed = func(result gateway.WarmupResult, err error) { warmErr = err; completed(result, err) }
	if err = s.Reload(context.Background(), 1); err != nil {
		t.Fatal(err)
	}
	for _, tc := range []struct {
		method, path string
		headers      map[string]string
		status       int
	}{
		{"OPTIONS", "/records/1", nil, 204},
		{"OPTIONS", "/records/1", map[string]string{"Origin": "https://client.example", "Access-Control-Request-Method": "GET", "Access-Control-Request-Headers": "X-Read"}, 204},
		{"OPTIONS", "/records/1", map[string]string{"Origin": "https://evil.example", "Access-Control-Request-Method": "GET"}, 403},
		{"GET", "/records/1", nil, 403},
		{"GET", "/records/1", map[string]string{"X-Component": "component-key"}, 403},
		{"GET", "/records/1", map[string]string{"X-Read": "read-key", "Origin": "https://client.example"}, 200},
		{"POST", "/warm/records/1", map[string]string{"X-Admin": "admin-key"}, 403},
		{"POST", "/warm/records/1", map[string]string{"X-Read": "read-key"}, 403},
		{"POST", "/warm/records/1", map[string]string{"X-Read": "read-key", "X-Admin": "admin-key"}, 200},
		{"GET", "/docs", nil, 403},
		{"GET", "/docs", map[string]string{"X-Document": "document-key"}, 200},
		{"GET", "/schema", map[string]string{"X-Document": "document-key"}, 200},
		{"GET", "/schema/records/1", map[string]string{"X-Document": "document-key"}, 403},
		{"GET", "/schema/records/1", map[string]string{"X-Document": "route-document-key"}, 200},
	} {
		t.Run(tc.method+tc.path, func(t *testing.T) {
			req := httptest.NewRequest(tc.method, tc.path, nil)
			for k, v := range tc.headers {
				req.Header.Set(k, v)
			}
			res := httptest.NewRecorder()
			s.manager.ServeHTTP(res, req)
			if res.Code != tc.status {
				t.Fatalf("%d %s warmup=%v", res.Code, res.Body.String(), warmErr)
			}
			if tc.status == 204 && tc.headers["Origin"] != "" {
				if res.Header().Get("Access-Control-Max-Age") != "600" || res.Header().Get("Access-Control-Allow-Credentials") != "" {
					t.Fatal(res.Header())
				}
			}
		})
	}
	if !strings.Contains(diagnostics.String(), "datly cache warmup completed") || strings.Contains(diagnostics.String(), "SELECT ") || strings.Contains(diagnostics.String(), "admin-key") {
		t.Fatalf("unsafe or absent completion: %s", diagnostics.String())
	}
	if err = s.exportDocuments(context.Background()); err != nil {
		t.Fatal(err)
	}
	jsonPath := filepath.Join(f.Root, "schema.json")
	data, err := os.ReadFile(jsonPath)
	if err != nil || !json.Valid(data) {
		t.Fatalf("export %v %s", err, data)
	}
	for _, secret := range []string{"read-key", "component-key", "admin-key", "document-key"} {
		if strings.Contains(string(data), secret) {
			t.Fatal("credential value leaked into documentation")
		}
	}
	if info, err := os.Stat(jsonPath); err != nil || info.Mode().Perm() != 0600 {
		t.Fatalf("private export mode %v %v", info, err)
	}
	if yaml, err := os.ReadFile(filepath.Join(f.Root, "record.yaml")); err != nil || !strings.Contains(string(yaml), "/records/{id}") {
		t.Fatalf("path export %v %s", err, yaml)
	}
	pinned, _, err := s.manager.Pin(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	age := int64(-1)
	s.source.http.CORS = &spec.CORS{MaxAge: &age}
	if err = s.Reload(context.Background(), 2); err == nil || s.manager.Revision() != 1 {
		t.Fatalf("invalid policy published: %v", err)
	}
	for _, ctx := range []context.Context{context.Background(), pinned} {
		res := httptest.NewRecorder()
		req := httptest.NewRequest("GET", "/records/1", nil).WithContext(ctx)
		req.Header.Set("X-Read", "read-key")
		s.manager.ServeHTTP(res, req)
		if res.Code != 200 {
			t.Fatalf("retained generation %d %s", res.Code, res.Body.String())
		}
	}
	after, err := os.ReadFile(jsonPath)
	if err != nil || string(after) != string(data) {
		t.Fatal("failed reload changed startup export")
	}
}
