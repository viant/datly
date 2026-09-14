package standalone

import (
	"context"
	"encoding/json"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/viant/datly/standalone/config"
	fixture "github.com/viant/datly/standalone/testdata/app"
	"github.com/viant/datly/standalone/testdata/app/records"
)

func TestConfiguredDisabledExportRetainsNativeCapture(t *testing.T) {
	t.Setenv("OTEL_EXPORTER_OTLP_ENDPOINT", "https://must-not-use.example")
	f := fixture.New(t)
	cfg, err := (config.Loader{}).Load(context.Background(), f.Config)
	if err != nil {
		t.Fatal(err)
	}
	cfg.Observation = &config.Observation{OTel: &config.OTel{Enabled: false}}
	exports, err := records.Exports()
	if err != nil {
		t.Fatal(err)
	}
	s, err := New(context.Background(), Options{Config: cfg, Registry: exports})
	if err != nil {
		t.Fatal(err)
	}
	defer s.Shutdown(context.Background())
	if err = s.Reload(context.Background(), 1); err != nil {
		t.Fatal(err)
	}
	res := httptest.NewRecorder()
	req := httptest.NewRequest("GET", "/records/1", nil)
	req.Header.Set("Datly-Show-Metrics", "true")
	s.manager.ServeHTTP(res, req)
	if res.Code != 200 {
		t.Fatal(res.Body.String())
	}
	found := false
	for name, values := range res.Header() {
		if strings.HasPrefix(name, "Datly-Metrics-") {
			found = true
			for _, value := range values {
				if !json.Valid([]byte(value)) || strings.Contains(value, "SELECT ") {
					t.Fatal("invalid or unsuppressed native capture")
				}
			}
		}
	}
	if !found {
		t.Fatal("native capture was disabled with export")
	}
}

func TestConfiguredExportFailureDoesNotReplaceFiles(t *testing.T) {
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
	s, err := New(context.Background(), Options{Config: cfg, Registry: exports})
	if err != nil {
		t.Fatal(err)
	}
	defer s.Shutdown(context.Background())
	if err = s.Reload(context.Background(), 1); err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(f.Root, "schema.json")
	if err = os.WriteFile(path, []byte("prior"), 0600); err != nil {
		t.Fatal(err)
	}
	s.source.http.OpenAPI.StartupExports[1].Path = "/unknown"
	if err = s.exportDocuments(context.Background()); err == nil {
		t.Fatal("unknown export path succeeded")
	}
	data, err := os.ReadFile(path)
	if err != nil || string(data) != "prior" {
		t.Fatal("rendering failure changed existing export")
	}
	s.source.http.OpenAPI.StartupExports[1].Path = "/records/{id}"
	s.source.http.OpenAPI.StartupExports[0].URL = filepath.Join(f.Root, "missing", "schema.json")
	if err = s.exportDocuments(context.Background()); err == nil {
		t.Fatal("missing parent destination accepted")
	}
	if err = s.Shutdown(context.Background()); err != nil {
		t.Fatal(err)
	}
	if err = s.source.connections.SQL.DB.Ping(); err == nil {
		t.Fatal("startup cleanup left connection open")
	}
}
