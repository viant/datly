package application_test

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"reflect"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/viant/datly/application"
	"github.com/viant/datly/bootstrap"
	"github.com/viant/datly/exec"
	gateway "github.com/viant/datly/gateway/http"
	"github.com/viant/datly/internal/testharness/sqlite"
	"github.com/viant/datly/runtime/registry"
	"github.com/viant/datly/spec"
	dsql "github.com/viant/datly/sql"
	"github.com/viant/datly/typecatalog"
	"github.com/viant/sqlx"
	xhandler "github.com/viant/xdatly/handler"
	"gopkg.in/yaml.v3"
)

type httpReloadInput struct{ Tenant int }
type httpReloadRow struct {
	ID int `sqlx:"id" json:"id"`
}
type httpReloadOutput struct {
	Rows []httpReloadRow `json:"rows"`
}

type httpReloadFixture struct {
	db    *sqlite.Harness
	cache string
}

func (f *httpReloadFixture) init(t *testing.T) {
	f.db = sqlite.New(t)
	f.cache = t.TempDir()
	if err := f.db.ExecStatements(context.Background(), "CREATE TABLE records(id INTEGER,tenant INTEGER)", "INSERT INTO records VALUES(11,1),(22,2)"); err != nil {
		t.Fatal(err)
	}
}

func (f *httpReloadFixture) compile(revision int, config gateway.Config, gate *httpPreparationGate) func(context.Context, *typecatalog.Catalog) (*application.Build, error) {
	return func(ctx context.Context, types *typecatalog.Catalog) (*application.Build, error) {
		required := true
		component := &spec.Component{
			Key:        spec.Key{Kind: spec.KindComponent, Scope: "example/http", Name: "Records"},
			Routes:     []*spec.Route{{Method: "GET", Path: "/records", APIKeyHeader: "X-Key", APIKeyValue: fmt.Sprint(revision)}},
			Settings:   &spec.Settings{Cache: &spec.CacheSettings{Enabled: true, Name: fmt.Sprintf("records-%d", revision), Location: f.cache, TTL: "1m", Warmup: &spec.CacheWarmupSettings{Cases: []*spec.CacheWarmupCase{{Set: []*spec.CacheWarmupParam{{Name: "Tenant", Values: []string{fmt.Sprint(revision)}}}}}}}},
			Parameters: []*spec.Parameter{{Name: "Tenant", TypeExpr: "int", Required: &required, Source: spec.BindSource{Kind: "query", Name: "tenant"}}, {Name: "Rows", Source: spec.BindSource{Kind: "output", Name: "view"}}},
			RootView:   &spec.View{Name: "records", Source: &spec.ViewSource{SQL: "SELECT id FROM records WHERE tenant=:Tenant ORDER BY id"}},
		}
		artifact, err := bootstrap.BuildArtifact(bootstrap.ArtifactInput{Component: component, Types: types, InputType: reflect.TypeFor[httpReloadInput](), OutputType: reflect.TypeFor[httpReloadOutput](), DirectViewField: "Rows"})
		if err != nil {
			return nil, err
		}
		reader, err := artifact.ReaderCompilation().NewExecution(bootstrap.ReaderRuntimeConfig{SQL: &dsql.SQLComponent{DB: f.db.DB}})
		if err != nil {
			return nil, err
		}
		if gate != nil {
			gate.Reader = reader
			gate.preparer = reader.(exec.QueryPreparer)
			gate.warmer = reader.(exec.ReaderWarmer)
			reader = gate
		}
		return &application.Build{HTTP: config, Components: []*registry.RegisteredComponent{{Component: artifact.Component, Input: artifact.Input, Output: artifact.Output, OutputType: reflect.TypeFor[httpReloadOutput](), Reader: reader}}}, nil
	}
}

func httpReloadConfig() gateway.Config {
	return gateway.Config{Warmup: &gateway.WarmupConfig{Timeout: time.Second, AdminHeaders: []string{"X-Admin"}, Authorize: func(ctx context.Context, req *http.Request, target exec.ComponentTarget) error {
		if req.Header.Get("X-Admin") != "admin" || target.Route.Path != "/records" {
			return fmt.Errorf("denied")
		}
		return nil
	}, Completed: func(gateway.WarmupResult, error) {}}}
}

type httpPreparationGate struct {
	exec.Reader
	preparer exec.QueryPreparer
	warmer   exec.ReaderWarmer
	started  chan context.Context
	release  chan struct{}
	calls    atomic.Int32
}

func (g *httpPreparationGate) PrepareQuery(ctx context.Context, input any, binder xhandler.Binder, resolver sqlx.ParameterResolver) (*exec.PreparedQuery, error) {
	if g.started != nil {
		select {
		case g.started <- ctx:
		default:
		}
		select {
		case <-g.release:
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}
	return g.preparer.PrepareQuery(ctx, input, binder, resolver)
}
func (g *httpPreparationGate) Warmup(ctx context.Context, request exec.ReaderWarmupInvocation) (int, error) {
	g.calls.Add(1)
	return g.warmer.Warmup(ctx, request)
}

func TestManagerHTTPConfigAtomicReloadSQLite(t *testing.T) {
	f := &httpReloadFixture{}
	f.init(t)
	manager, err := application.New(nil)
	if err != nil {
		t.Fatal(err)
	}
	defer manager.Shutdown(context.Background())
	config := httpReloadConfig()
	if err := manager.Reload(context.Background(), application.Request{Revision: 1, Compile: f.compile(1, config, nil)}); err != nil {
		t.Fatal(err)
	}
	for _, mode := range []string{"invalid CORS", "invalid URI", "missing admin", "external lifetime"} {
		t.Run(mode, func(t *testing.T) {
			invalid := httpReloadConfig()
			switch mode {
			case "invalid CORS":
				age := int64(-1)
				invalid.CORS = &spec.CORS{MaxAge: &age}
			case "invalid URI":
				invalid.Meta.CacheWarmURI = "/warm?bad"
			case "missing admin":
				invalid.Warmup.Authorize = nil
			case "external lifetime":
				invalid.Warmup.Lifetime = gateway.NewWarmupLifetime(context.Background())
				defer invalid.Warmup.Lifetime.Shutdown(context.Background())
			}
			if err := manager.Reload(context.Background(), application.Request{Revision: 2, Compile: f.compile(2, invalid, nil)}); err == nil {
				t.Fatal("invalid HTTP policy published")
			}
			if manager.Revision() != 1 {
				t.Fatal("invalid stage replaced old generation")
			}
			req := httptest.NewRequest("POST", gateway.DefaultCacheWarmURI+"/records", nil)
			req.Header.Set("X-Key", "1")
			req.Header.Set("X-Admin", "admin")
			req.Header.Set("Origin", "https://default.example")
			res := httptest.NewRecorder()
			manager.ServeHTTP(res, req)
			if res.Code != 200 || res.Header().Get("Access-Control-Allow-Origin") != "https://default.example" {
				t.Fatalf("old configured handler lost: %d %s %v", res.Code, res.Body.String(), res.Header())
			}
		})
	}
}

func TestManagerHTTPConfigDecodedDefaultsAndPresenceSQLite(t *testing.T) {
	for _, tc := range []struct {
		name, config        string
		origin, credentials string
		warmStatus          int
	}{
		{"default noncredentialed CORS", `{}`, "https://test.example", "", 200},
		{"explicit empty CORS", `{"CORS":{}}`, "", "", 200},
		{"disable overrides route", `{"DisableCors":true}`, "", "", 200},
		{"false and empty methods", `{"CORS":{"AllowOrigins":["*"],"AllowCredentials":false,"AllowMethods":[]}}`, "", "", 200},
		{"explicit credentialed origin", `{"CORS":{"AllowOrigins":["https://test.example"],"AllowCredentials":true}}`, "https://test.example", "true", 200},
		{"false credentials", `{"CORS":{"AllowOrigins":["*"],"AllowCredentials":false}}`, "https://test.example", "", 200},
		{"whitespace disables warmup", `{"Meta":{"CacheWarmURI":" "}}`, "https://test.example", "", 404},
	} {
		t.Run(tc.name, func(t *testing.T) {
			f := &httpReloadFixture{}
			f.init(t)
			manager, err := application.New(nil)
			if err != nil {
				t.Fatal(err)
			}
			defer manager.Shutdown(context.Background())
			var config gateway.Config
			if err := json.Unmarshal([]byte(tc.config), &config); err != nil {
				t.Fatal(err)
			}
			config.Warmup = httpReloadConfig().Warmup
			compile := f.compile(1, config, nil)
			if tc.name == "disable overrides route" {
				compile = func(ctx context.Context, catalog *typecatalog.Catalog) (*application.Build, error) {
					built, err := f.compile(1, config, nil)(ctx, catalog)
					if err == nil {
						built.Components[0].Component.Routes[0].CORS = spec.DefaultCORS()
					}
					return built, err
				}
			}
			if err := manager.Reload(context.Background(), application.Request{Revision: 1, Compile: compile}); err != nil {
				t.Fatal(err)
			}
			req := httptest.NewRequest("GET", "/records?tenant=1", nil)
			req.Header.Set("X-Key", "1")
			req.Header.Set("Origin", "https://test.example")
			res := httptest.NewRecorder()
			manager.ServeHTTP(res, req)
			if res.Code != 200 || res.Header().Get("Access-Control-Allow-Origin") != tc.origin || res.Header().Get("Access-Control-Allow-Credentials") != tc.credentials {
				t.Fatalf("%d %v", res.Code, res.Header())
			}
			warm := httptest.NewRequest("POST", gateway.DefaultCacheWarmURI+"/records", nil)
			warm.Header.Set("X-Key", "1")
			warm.Header.Set("X-Admin", "admin")
			res = httptest.NewRecorder()
			manager.ServeHTTP(res, warm)
			if res.Code != tc.warmStatus {
				t.Fatalf("warmup=%d %s", res.Code, res.Body.String())
			}
		})
	}
}

func TestManagerWarmupReloadAndPinnedLifetimeSQLite(t *testing.T) {
	f := &httpReloadFixture{}
	f.init(t)
	manager, err := application.New(nil)
	if err != nil {
		t.Fatal(err)
	}
	defer manager.Shutdown(context.Background())
	gate := &httpPreparationGate{started: make(chan context.Context, 1), release: make(chan struct{})}
	if err := manager.Reload(context.Background(), application.Request{Revision: 1, Compile: f.compile(1, httpReloadConfig(), gate)}); err != nil {
		t.Fatal(err)
	}
	pinned, _, err := manager.Pin(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	caller, cancel := context.WithCancel(pinned)
	defer cancel()
	old := httptest.NewRequest("POST", gateway.DefaultCacheWarmURI+"/records", nil).WithContext(caller)
	old.Header.Set("X-Key", "1")
	old.Header.Set("X-Admin", "admin")
	oldResponse := httptest.NewRecorder()
	done := make(chan struct{})
	go func() { defer close(done); manager.ServeHTTP(oldResponse, old) }()
	select {
	case <-gate.started:
	case <-time.After(5 * time.Second):
		t.Fatal("preparation not entered")
	}
	cancel()
	config := httpReloadConfig()
	config.Meta.CacheWarmURI = "/configured"
	if err := manager.Reload(context.Background(), application.Request{Revision: 2, Compile: f.compile(2, config, nil)}); err != nil {
		t.Fatal(err)
	}
	current := httptest.NewRequest("POST", "/configured/records", nil)
	current.Header.Set("X-Key", "2")
	current.Header.Set("X-Admin", "admin")
	res := httptest.NewRecorder()
	manager.ServeHTTP(res, current)
	if res.Code != 200 {
		t.Fatalf("new warmup=%d %s", res.Code, res.Body.String())
	}
	close(gate.release)
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("old warmup orphaned")
	}
	if oldResponse.Code != 200 || gate.calls.Load() != 1 {
		t.Fatalf("old warmup canceled: %d %d", oldResponse.Code, gate.calls.Load())
	}
	if err := f.db.ExecStatements(context.Background(), "DROP TABLE records"); err != nil {
		t.Fatal(err)
	}
	for _, tc := range []struct {
		ctx       context.Context
		key, want string
	}{{pinned, "1", "11"}, {context.Background(), "2", "22"}} {
		req := httptest.NewRequest("GET", "/records?tenant="+tc.key, nil).WithContext(tc.ctx)
		req.Header.Set("X-Key", tc.key)
		res := httptest.NewRecorder()
		manager.ServeHTTP(res, req)
		if res.Code != 200 || !strings.Contains(res.Body.String(), tc.want) {
			t.Fatalf("generation cache lost: %d %s", res.Code, res.Body.String())
		}
	}
	if err := manager.Shutdown(context.Background()); err != nil {
		t.Fatal(err)
	}
	rejected := httptest.NewRecorder()
	manager.ServeHTTP(rejected, old.WithContext(pinned))
	if rejected.Code != 503 {
		t.Fatal("pinned generation bypassed shutdown")
	}
	if err := manager.Reload(context.Background(), application.Request{Revision: 3, Compile: f.compile(3, config, nil)}); !errors.Is(err, application.ErrClosed) {
		t.Fatalf("reload after shutdown=%v", err)
	}
}

func TestManagerYAMLHTTPPolicySQLite(t *testing.T) {
	f := &httpReloadFixture{}
	f.init(t)
	manager, err := application.New(nil)
	if err != nil {
		t.Fatal(err)
	}
	defer manager.Shutdown(context.Background())
	var config gateway.Config
	if err := yaml.Unmarshal([]byte(`DisableCors: false
CORS:
  AllowOrigins: [https://app.example]
  AllowMethods: [GET, POST]
  AllowHeaders: []
  ExposeHeaders: [X-Result]
  AllowCredentials: false
  MaxAge: 0
Meta:
  CacheWarmURI: /ops/cache
`), &config); err != nil {
		t.Fatal(err)
	}
	if config.CORS.AllowHeaders == nil || len(*config.CORS.AllowHeaders) != 0 || config.CORS.AllowCredentials == nil || *config.CORS.AllowCredentials || config.CORS.MaxAge == nil || *config.CORS.MaxAge != 0 {
		t.Fatal("YAML lost authored presence")
	}
	config.Warmup = httpReloadConfig().Warmup
	if err := manager.Reload(context.Background(), application.Request{Revision: 1, Compile: f.compile(1, config, nil)}); err != nil {
		t.Fatal(err)
	}
	req := httptest.NewRequest("OPTIONS", "/records", nil)
	req.Header.Set("Origin", "https://app.example")
	req.Header.Set("Access-Control-Request-Method", "GET")
	res := httptest.NewRecorder()
	manager.ServeHTTP(res, req)
	if res.Code != 204 || res.Header().Get("Access-Control-Max-Age") != "0" || res.Header().Get("Access-Control-Allow-Credentials") != "" {
		t.Fatalf("YAML preflight=%d %v", res.Code, res.Header())
	}
	req = httptest.NewRequest("POST", "/ops/cache/records", nil)
	req.Header.Set("X-Key", "1")
	req.Header.Set("X-Admin", "admin")
	res = httptest.NewRecorder()
	manager.ServeHTTP(res, req)
	if res.Code != 200 {
		t.Fatalf("YAML warmup route=%d %s", res.Code, res.Body.String())
	}
}
