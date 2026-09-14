package standalone

import (
	"context"
	"fmt"
	"github.com/viant/bindly/resource"
	"github.com/viant/datly/internal/testharness"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"testing/fstest"
	"time"

	"github.com/viant/afs"
	gateway "github.com/viant/datly/gateway/http"
	"github.com/viant/datly/gateway/openapi/openapi3"
	"github.com/viant/datly/spec"
	"github.com/viant/datly/standalone/config"
	fixture "github.com/viant/datly/standalone/testdata/app"
	"github.com/viant/datly/standalone/testdata/app/records"
)

func TestStaticConfiguredLocalHTTPReload(t *testing.T) {
	ctx := context.Background()
	root := staticPhysicalDir(t)
	write := func(name, data string) {
		t.Helper()
		p := filepath.Join(root, name)
		if err := os.MkdirAll(filepath.Dir(p), 0755); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(p, []byte(data), 0644); err != nil {
			t.Fatal(err)
		}
	}
	write("site/index.html", "<html>home</html>")
	write("site/nested/index.html", "nested")
	write("site/nested/data.txt", "abcdef")
	write("site/space name.txt", "space")
	write("secret.txt", "outside")
	cfgFile := filepath.Join(root, "config.json")
	if err := os.WriteFile(cfgFile, []byte(`{"ContentURL":".","StaticContent":[{"Path":"/ui","ContentURL":"site"}],"APIKeys":[{"URI":"/ui","Header":"X-Key","Value":"secret"}]}`), 0644); err != nil {
		t.Fatal(err)
	}
	cfg, err := (config.Loader{}).Load(ctx, cfgFile)
	if err != nil {
		t.Fatal(err)
	}
	server, err := New(ctx, Options{Config: cfg})
	if err != nil {
		t.Fatal(err)
	}
	defer server.Shutdown(ctx)
	if err = server.Reload(ctx, 1); err != nil {
		t.Fatal(err)
	}
	request := func(method, target, key, rangeHeader string) *httptest.ResponseRecorder {
		t.Helper()
		r := httptest.NewRequest(method, target, nil)
		r.Header.Set("Origin", "https://example.test")
		r.Header.Set("X-Key", key)
		if rangeHeader != "" {
			r.Header.Set("Range", rangeHeader)
		}
		w := httptest.NewRecorder()
		server.ServeHTTP(w, r)
		return w
	}
	for _, tc := range []struct {
		method, path, key, rangeHeader string
		status                         int
		body                           string
	}{
		{"GET", "/ui/", "secret", "", 200, "<html>home</html>"},
		{"HEAD", "/ui/nested/data.txt", "secret", "", 200, ""},
		{"GET", "/ui/nested/data.txt", "secret", "bytes=1-3", 206, "bcd"},
		{"GET", "/ui/nested/", "secret", "", 200, "nested"},
		{"GET", "/ui/space%20name.txt", "secret", "", 200, "space"},
		{"GET", "/ui/missing", "secret", "", 404, ""},
		{"GET", "/ui/.", "secret", "", 404, ""},
		{"GET", "/ui//", "secret", "", 404, ""},
		{"GET", "/ui/../secret.txt", "secret", "", 404, ""},
		{"GET", "/ui/%2e%2e/secret.txt", "secret", "", 404, ""},
		{"GET", "/ui/nested%2f..%2f..%2fsecret.txt", "secret", "", 404, ""},
		{"GET", "/ui/%252e%252e/secret.txt", "secret", "", 404, ""},
		{"GET", "/ui/a%5csecret.txt", "secret", "", 404, ""},
		{"GET", "/ui/", "", "", 403, ""},
		{"POST", "/ui/", "secret", "", 405, ""},
		{"GET", "/uix/", "secret", "", 404, ""},
		{"GET", "/ui", "secret", "", 301, ""},
		{"GET", "/ui/nested", "secret", "", 301, ""},
		{"GET", "/ui/index.html", "secret", "", 301, ""},
		{"GET", "/ui/nested/data.txt", "secret", "bytes=90-100", 416, ""},
	} {
		t.Run(tc.method+tc.path+tc.rangeHeader+tc.key, func(t *testing.T) {
			w := request(tc.method, tc.path, tc.key, tc.rangeHeader)
			if w.Code != tc.status || tc.body != "" && w.Body.String() != tc.body || tc.method == "HEAD" && w.Body.Len() != 0 {
				t.Fatalf("status=%d body=%q headers=%v", w.Code, w.Body.String(), w.Header())
			}
		})
	}
	w := request("HEAD", "/ui/nested/data.txt", "secret", "")
	if w.Header().Get("Content-Length") != "6" || !strings.HasPrefix(w.Header().Get("Content-Type"), "text/plain") || w.Header().Get("Access-Control-Allow-Origin") == "" {
		t.Fatalf("headers=%v", w.Header())
	}
	r := httptest.NewRequest("OPTIONS", "/ui/nested/data.txt", nil)
	r.Header.Set("Origin", "https://example.test")
	r.Header.Set("Access-Control-Request-Method", "GET")
	r.Header.Set("Access-Control-Request-Headers", "X-Key")
	w = httptest.NewRecorder()
	server.ServeHTTP(w, r)
	if w.Code != 204 {
		t.Fatalf("preflight: %d", w.Code)
	}
	write("site/nested/data.txt", "new-generation")
	if got := request("GET", "/ui/nested/data.txt", "secret", "").Body.String(); got != "abcdef" {
		t.Fatalf("active snapshot mutated: %q", got)
	}
	if err = server.Reload(ctx, 2); err != nil {
		t.Fatal(err)
	}
	if got := request("GET", "/ui/nested/data.txt", "secret", "").Body.String(); got != "new-generation" {
		t.Fatalf("reload: %q", got)
	}
	if err = os.Symlink(filepath.Join(root, "secret.txt"), filepath.Join(root, "site", "escape.txt")); err != nil {
		t.Fatal(err)
	}
	if err = server.Reload(ctx, 3); err == nil {
		t.Fatal("escaping symlink accepted")
	}
	if server.manager.Revision() != 2 {
		t.Fatal("failed reload changed revision")
	}
	if got := request("GET", "/ui/nested/data.txt", "secret", "").Body.String(); got != "new-generation" {
		t.Fatal("failed reload lost old bytes")
	}
}

func TestStaticConfiguredAFS(t *testing.T) {
	ctx := context.Background()
	location := "mem://static-acceptance/" + t.Name()
	fs := afs.New()
	for name, data := range map[string]string{"site/index.html": "remote", "site/nested/data.txt": "abcdef"} {
		if err := fs.Upload(ctx, location+"/"+name, 0644, strings.NewReader(data)); err != nil {
			t.Fatal(err)
		}
	}
	cfg := &config.Config{Config: gateway.Config{ContentURL: location, StaticContent: []*spec.StaticContent{{Path: "/remote", ContentURL: "site"}}}}
	s, err := New(ctx, Options{Config: cfg})
	if err != nil {
		t.Fatal(err)
	}
	defer s.Shutdown(ctx)
	if err = s.Reload(ctx, 1); err != nil {
		t.Fatal(err)
	}
	for _, target := range []string{"/remote/", "/remote/nested/data.txt"} {
		r := httptest.NewRequest("GET", target, nil)
		if strings.HasSuffix(target, "txt") {
			r.Header.Set("Range", "bytes=2-4")
		}
		w := httptest.NewRecorder()
		s.ServeHTTP(w, r)
		if w.Code != 200 && w.Code != 206 {
			t.Fatalf("%s: %d %s", target, w.Code, w.Body.String())
		}
		if strings.HasSuffix(target, "txt") && w.Body.String() != "cde" {
			t.Fatal(w.Body.String())
		}
	}
}

func TestStaticComponentPrecedenceAndConflicts(t *testing.T) {
	f := fixture.New(t)
	ctx := context.Background()
	root := staticPhysicalDir(t)
	if err := os.WriteFile(filepath.Join(root, "index.html"), []byte("static"), 0644); err != nil {
		t.Fatal(err)
	}
	cfg, err := (config.Loader{}).Load(ctx, f.Config)
	if err != nil {
		t.Fatal(err)
	}
	exports, err := records.Exports()
	if err != nil {
		t.Fatal(err)
	}
	cfg.StaticContent = []*spec.StaticContent{{Path: "/", ContentURL: root}}
	s, err := New(ctx, Options{Config: cfg, Registry: exports})
	if err != nil {
		t.Fatal(err)
	}
	defer s.Shutdown(ctx)
	if err = s.Reload(ctx, 1); err != nil {
		t.Fatal(err)
	}
	for _, tc := range []struct {
		method, target string
		status         int
	}{{"GET", "/records/1", 200}, {"DELETE", "/records", 405}, {"GET", "/", 200}} {
		w := httptest.NewRecorder()
		s.ServeHTTP(w, httptest.NewRequest(tc.method, tc.target, nil))
		if w.Code != tc.status {
			t.Fatalf("%s %s: %d %s", tc.method, tc.target, w.Code, w.Body.String())
		}
	}
	// Actual configured admin/document prefixes are reserved even in static-only applications.
	for _, prefix := range []string{"/records", "/docs", "/docs/child", "/admin"} {
		t.Run(prefix, func(t *testing.T) {
			candidateConfig := *cfg
			candidateConfig.StaticContent = []*spec.StaticContent{{Path: prefix, ContentURL: root}}
			candidateConfig.Meta.OpenApiURI = "/docs"
			candidateConfig.Meta.CacheWarmURI = "/admin"
			candidate, err := New(ctx, Options{Config: &candidateConfig, Registry: exports})
			if err != nil {
				t.Fatal(err)
			}
			defer candidate.Shutdown(ctx)
			if err = candidate.Reload(ctx, 1); err == nil {
				t.Fatal("conflict accepted")
			}
		})
	}
}

func TestStaticRootConfinement(t *testing.T) {
	ctx := context.Background()
	root := staticPhysicalDir(t)
	outside := staticPhysicalDir(t)
	os.WriteFile(filepath.Join(outside, "secret"), []byte("secret"), 0644)
	os.Symlink(outside, filepath.Join(root, "escape"))
	for _, tc := range []struct{ url, sub string }{{"../" + filepath.Base(outside), ""}, {"escape", ""}, {".", "escape"}, {"%2e%2e", ""}, {".", "../outside"}} {
		t.Run(fmt.Sprint(tc), func(t *testing.T) {
			cfg := &config.Config{Config: gateway.Config{ContentURL: root, StaticContent: []*spec.StaticContent{{Path: "/", ContentURL: tc.url, Root: tc.sub}}}}
			s, err := New(ctx, Options{Config: cfg})
			if err != nil {
				t.Fatal(err)
			}
			defer s.Shutdown(ctx)
			if err = s.Reload(ctx, 1); err == nil {
				t.Fatal("escaping root accepted")
			}
		})
	}
}

var _ http.Handler = (*Server)(nil)

func TestStaticDQLStandaloneDiscovery(t *testing.T) {
	ctx := context.Background()
	root := staticPhysicalDir(t)
	module := "example.com/static"
	(testharness.GeneratedModule{Path: module}).Write(t, root)
	for name, data := range map[string]string{"site.dql": "#setting($_ = $route('/static','GET'))\n#setting($_ = $static_content('assets','.'))", "assets/index.html": "authored-static"} {
		if err := os.MkdirAll(filepath.Dir(filepath.Join(root, name)), 0755); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(filepath.Join(root, name), []byte(data), 0644); err != nil {
			t.Fatal(err)
		}
	}
	cfg := &config.Config{BaseDir: root, GoBootstrap: &config.Packages{Packages: []string{module}}, Config: gateway.Config{ContentURL: root}}
	s, err := New(ctx, Options{Config: cfg})
	if err != nil {
		t.Fatal(err)
	}
	defer s.Shutdown(ctx)
	if err = s.Reload(ctx, 1); err != nil {
		t.Fatal(err)
	}
	w := httptest.NewRecorder()
	s.ServeHTTP(w, httptest.NewRequest("GET", "/static/", nil))
	if w.Code != 200 || w.Body.String() != "authored-static" {
		t.Fatalf("%d %q", w.Code, w.Body.String())
	}
}

func TestStaticListener(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	root := staticPhysicalDir(t)
	if err := os.WriteFile(filepath.Join(root, "index.html"), []byte("socket-static"), 0644); err != nil {
		t.Fatal(err)
	}
	cfg := &config.Config{Endpoint: config.Endpoint{Address: "127.0.0.1:0"}, Config: gateway.Config{StaticContent: []*spec.StaticContent{{Path: "/site", ContentURL: root}}}}
	s, err := New(ctx, Options{Config: cfg})
	if err != nil {
		t.Fatal(err)
	}
	defer s.Shutdown(context.Background())
	ready := &staticReady{addresses: make(chan string, 1)}
	done := make(chan error, 1)
	go func() { done <- s.Serve(ctx, ready) }()
	var address string
	select {
	case address = <-ready.addresses:
	case err := <-done:
		t.Fatalf("listener startup: %v", err)
	case <-time.After(15 * time.Second):
		t.Fatal("listener readiness timeout")
	}
	res, err := http.Get("http://" + address + "/site/")
	if err != nil {
		t.Fatal(err)
	}
	defer res.Body.Close()
	data, err := io.ReadAll(res.Body)
	if err != nil {
		t.Fatal(err)
	}
	if res.StatusCode != 200 || string(data) != "socket-static" {
		t.Fatalf("%d %q", res.StatusCode, data)
	}
	cancel()
	if err = <-done; err != nil {
		t.Fatal(err)
	}
}

type staticReady struct{ addresses chan string }

func (r *staticReady) Write(data []byte) (int, error) {
	r.addresses <- strings.TrimSpace(strings.TrimPrefix(string(data), "HTTP listening on "))
	return len(data), nil
}

func TestStaticDocumentPrecedence(t *testing.T) {
	ctx := context.Background()
	f := fixture.New(t)
	cfg, err := (config.Loader{}).Load(ctx, f.Config)
	if err != nil {
		t.Fatal(err)
	}
	exports, err := records.Exports()
	if err != nil {
		t.Fatal(err)
	}
	root := staticPhysicalDir(t)
	if err = os.WriteFile(filepath.Join(root, "docs"), []byte("must-not-shadow-document"), 0644); err != nil {
		t.Fatal(err)
	}
	cfg.StaticContent = []*spec.StaticContent{{Path: "/", ContentURL: root}}
	cfg.Meta.OpenApiURI = "/docs"
	cfg.Meta.DocURI = "/doc-ui"
	cfg.OpenAPI = &gateway.OpenAPIConfig{Info: openapi3.Info{Title: "Static acceptance", Version: "1"}, AggregateAccess: &gateway.DocumentAccess{APIKeyHeader: "X-Docs", APIKeyValue: "docs"}}
	s, err := New(ctx, Options{Config: cfg, Registry: exports})
	if err != nil {
		t.Fatal(err)
	}
	defer s.Shutdown(ctx)
	if err = s.Reload(ctx, 1); err != nil {
		t.Fatal(err)
	}
	for _, tc := range []struct {
		target, key string
		status      int
	}{{"/docs", "", 403}, {"/docs", "docs", 200}, {"/%64ocs", "", 404}, {"/docs/missing", "", 404}} {
		r := httptest.NewRequest("GET", tc.target, nil)
		r.Header.Set("X-Docs", tc.key)
		w := httptest.NewRecorder()
		s.ServeHTTP(w, r)
		if w.Code != tc.status || strings.Contains(w.Body.String(), "must-not-shadow-document") {
			t.Fatalf("%s: %d %q", tc.target, w.Code, w.Body.String())
		}
	}
}

func TestStaticLinkedResourcesWithComponents(t *testing.T) {
	ctx := context.Background()
	f := fixture.New(t)
	cfg, err := (config.Loader{}).Load(ctx, f.Config)
	if err != nil {
		t.Fatal(err)
	}
	exports, err := records.Exports()
	if err != nil {
		t.Fatal(err)
	}
	store := resource.New()
	if err = store.Register("web", fstest.MapFS{"a/index.html": &fstest.MapFile{Data: []byte("a")}, "b/index.html": &fstest.MapFile{Data: []byte("b")}}); err != nil {
		t.Fatal(err)
	}
	cfg.StaticContent = []*spec.StaticContent{{Path: "/a", Namespace: "web", Root: "a"}, {Path: "/b", Namespace: "web", Root: "b"}}
	s, err := New(ctx, Options{Config: cfg, Registry: exports, Resources: store})
	if err != nil {
		t.Fatal(err)
	}
	defer s.Shutdown(ctx)
	if err = s.Reload(ctx, 1); err != nil {
		t.Fatal(err)
	}
	for _, name := range []string{"a", "b"} {
		w := httptest.NewRecorder()
		s.ServeHTTP(w, httptest.NewRequest("GET", "/"+name+"/", nil))
		if w.Code != 200 || w.Body.String() != name {
			t.Fatalf("%d %q", w.Code, w.Body.String())
		}
	}
}

func TestStaticRejectsSymlinkAuthority(t *testing.T) {
	ctx := context.Background()
	outside, base := staticPhysicalDir(t), staticPhysicalDir(t)
	if err := os.WriteFile(filepath.Join(outside, "index.html"), []byte("secret"), 0600); err != nil {
		t.Fatal(err)
	}
	link := filepath.Join(base, "site")
	if err := os.Symlink(outside, link); err != nil {
		t.Fatal(err)
	}
	for _, mode := range []string{"direct", "file_url", "global"} {
		t.Run(mode, func(t *testing.T) {
			cfg := &config.Config{Config: gateway.Config{StaticContent: []*spec.StaticContent{{Path: "/site", ContentURL: link}}}}
			if mode == "file_url" {
				cfg.StaticContent[0].ContentURL = "file://" + link
			}
			if mode == "global" {
				cfg.ContentURL = link
				cfg.StaticContent[0].ContentURL = "."
			}
			server, err := New(ctx, Options{Config: cfg})
			if err != nil {
				t.Fatal(err)
			}
			defer server.Shutdown(ctx)
			if err := server.Reload(ctx, 1); err == nil {
				t.Fatal("symlink authority was published")
			}
		})
	}
}
