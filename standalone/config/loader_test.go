package config_test

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/viant/afs"
	_ "github.com/viant/afs/mem"
	"github.com/viant/datly/standalone/config"
)

func TestLoaderJSONYAMLRelativeAndAFS(t *testing.T) {
	for _, format := range []string{"json", "yaml", "yml"} {
		t.Run(format, func(t *testing.T) {
			root := t.TempDir()
			var data []byte
			if format == "json" {
				data = []byte(`{"GoBootstrap":{"Packages":["example.com/app/api"]},"BaseDir":"app","DependencyURL":"deps","RouteURL":"routes","ContentURL":"content","PluginsURL":"plugins","JobURL":"jobs","FailedJobURL":"failed","Connectors":[{"Name":"inline","Driver":"mysql","DSN":"user:private-password@tcp(host)/db"}],"JWTValidator":{"CertURL":"https://keys.example/certs"}}`)
			} else {
				data = []byte("GoBootstrap:\n  Packages: [example.com/app/api]\nBaseDir: app\nDependencyURL: deps\nRouteURL: routes\nContentURL: content\nPluginsURL: plugins\nJobURL: jobs\nFailedJobURL: failed\nConnectors:\n  - Name: inline\n    Driver: mysql\n    DSN: user:private-password@tcp(host)/db\nJWTValidator:\n  CertURL: https://keys.example/certs\n")
			}
			if err := os.Mkdir(filepath.Join(root, "deps"), 0700); err != nil {
				t.Fatal(err)
			}
			if err := os.WriteFile(filepath.Join(root, "deps/connectors.yaml"), []byte("Connectors:\n  - Name: main\n    Driver: sqlite3\n    DSN: unchanged.db\n"), 0600); err != nil {
				t.Fatal(err)
			}
			location := filepath.Join(root, "config."+format)
			if err := os.WriteFile(location, data, 0600); err != nil {
				t.Fatal(err)
			}
			for _, url := range []string{location, "file://" + location} {
				cfg, err := (config.Loader{FS: afs.New()}).Load(context.Background(), url)
				if err != nil {
					t.Fatal(err)
				}
				if cfg.BaseDir != filepath.Join(root, "app") || len(cfg.Connectors) != 2 || cfg.Connectors[0].DSN != "user:private-password@tcp(host)/db" || cfg.Connectors[1].DSN != "unchanged.db" || cfg.JWTValidator.CertURL != "https://keys.example/certs" {
					t.Fatalf("configuration fields were not preserved")
				}
				for value, suffix := range map[string]string{cfg.RouteURL: "routes", cfg.ContentURL: "content", cfg.PluginsURL: "plugins", cfg.DependencyURL: "deps", cfg.JobURL: "jobs", cfg.FailedJobURL: "failed"} {
					if !strings.HasSuffix(value, root+"/"+suffix) {
						t.Fatalf("relative location %s", value)
					}
				}
				if cfg.Validate() == nil {
					t.Fatal("unsupported deployment was silently accepted")
				}
			}
		})
	}
}

func TestLoaderHTTPAFSAndCancellation(t *testing.T) {
	root := t.TempDir()
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/cfg/app.yaml" {
			w.Write([]byte("BaseDir: " + root + "\nGoBootstrap:\n  Packages: [example.com/app/api]\nDependencyURL: connectors.json\n"))
			return
		}
		if r.URL.Path == "/cfg/connectors.json" {
			w.Write([]byte(`{"Connectors":[{"Name":"main","Driver":"sqlite3","DSN":"retained"}]}`))
			return
		}
		http.NotFound(w, r)
	}))
	defer server.Close()
	cfg, err := (config.Loader{}).Load(context.Background(), server.URL+"/cfg/app.yaml?credential=do-not-print")
	if err != nil {
		t.Fatal(err)
	}
	if len(cfg.Connectors) != 1 || cfg.DependencyURL != server.URL+"/cfg/connectors.json" {
		t.Fatalf("relative AFS dependency was not loaded")
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err = (config.Loader{}).Load(ctx, server.URL); !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
}

func TestLoaderRejectsMalformedUnknownAndMultipleDocuments(t *testing.T) {
	for _, tc := range []struct{ ext, data string }{{"json", `{"Password-private":"sensitive"}`}, {"json", `null`}, {"json", `{} {}`}, {"yaml", "{}\n---\n{}"}, {"yaml", "Endpoint:\n  Port: private-password"}, {"yaml", "GoBootstrap: {}\nGoBootstrap: {}"}} {
		t.Run(tc.ext+tc.data, func(t *testing.T) {
			name := filepath.Join(t.TempDir(), "config."+tc.ext)
			if err := os.WriteFile(name, []byte(tc.data), 0600); err != nil {
				t.Fatal(err)
			}
			_, err := (config.Loader{}).Load(context.Background(), name)
			if err == nil || strings.Contains(err.Error(), "private") || strings.Contains(err.Error(), "sensitive") {
				t.Fatalf("unsafe/absent error %v", err)
			}
		})
	}
}

func TestEndpointDefaultsAndValidation(t *testing.T) {
	address, err := (config.Endpoint{}).ListenAddress()
	if err != nil || address != ":8080" {
		t.Fatalf("%s %v", address, err)
	}
	for _, e := range []config.Endpoint{{Port: -1}, {Port: 65536}, {ReadTimeoutMs: int(^uint(0) >> 1)}, {Address: "127.0.0.1"}, {Address: "127.0.0.1:0", Port: 8080}, {ShutdownTimeoutMs: int(^uint(0) >> 1)}} {
		if _, err := e.ListenAddress(); err == nil {
			t.Fatalf("invalid endpoint accepted: %+v", e)
		}
	}
	if _, err := (config.Endpoint{ReadTimeoutMs: -1, WriteTimeoutMs: -1, MaxHeaderBytes: -1}).ListenAddress(); err != nil {
		t.Fatal("nonpositive original HTTP limits should retain net/http semantics")
	}
	var cfg config.Config
	if err := json.Unmarshal([]byte(`{"DisableCors":true,"Endpoint":{"Port":9090,"ReadTimeoutMs":12,"WriteTimeoutMs":34,"MaxHeaderBytes":4096},"GoBootstrap":{"Packages":["example.com/app/api"]}}`), &cfg); err != nil {
		t.Fatal(err)
	}
	if err := cfg.Validate(); err != nil {
		t.Fatal(err)
	}
	if !cfg.DisableCors || cfg.Endpoint.ReadTimeoutMs != 12 || cfg.Endpoint.WriteTimeoutMs != 34 || cfg.Endpoint.MaxHeaderBytes != 4096 {
		t.Fatal("endpoint options lost")
	}
}

func TestLoaderMemoryAFSRelative(t *testing.T) {
	fs := afs.New()
	base := "mem://localhost/" + filepath.Base(t.TempDir())
	ctx := context.Background()
	defer fs.Delete(ctx, base)
	if err := fs.Upload(ctx, base+"/connectors.json", 0600, strings.NewReader(`{"Connectors":[{"Name":"main","Driver":"sqlite3","DSN":"retained"}]}`)); err != nil {
		t.Fatal(err)
	}
	content := `{"BaseDir":"` + t.TempDir() + `","GoBootstrap":{"Packages":["example.com/app/api"]},"DependencyURL":"connectors.json"}`
	if err := fs.Upload(ctx, base+"/config.json", 0600, strings.NewReader(content)); err != nil {
		t.Fatal(err)
	}
	cfg, err := (config.Loader{FS: fs}).Load(ctx, base+"/config.json")
	if err != nil {
		t.Fatal(err)
	}
	if cfg.DependencyURL != base+"/connectors.json" || len(cfg.Connectors) != 1 || cfg.Connectors[0].DSN != "retained" {
		t.Fatal("AFS relative configuration was not preserved")
	}
}
