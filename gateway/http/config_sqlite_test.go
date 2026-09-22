package http

import (
	"context"
	"encoding/json"
	"errors"
	stdhttp "net/http"
	"net/http/httptest"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/viant/datly/bootstrap"
	dexec "github.com/viant/datly/exec"
	"github.com/viant/datly/internal/testharness/sqlite"
	druntime "github.com/viant/datly/runtime"
	"github.com/viant/datly/spec"
	dsql "github.com/viant/datly/sql"
	"github.com/viant/sqlx/io/read/cache/aerospike"
)

type configInput struct{ Tenant int }
type configRow struct {
	ID int `sqlx:"id" json:"id"`
}
type configOutput struct {
	Rows []configRow `json:"rows"`
}

type configFixture struct {
	pool      *aerospike.Pool
	db        *sqlite.Harness
	component *spec.Component
}

func newConfigFixture(t *testing.T) *configFixture {
	t.Helper()
	db := sqlite.New(t)
	if err := db.ExecStatements(context.Background(), "CREATE TABLE records(id INTEGER,tenant INTEGER)", "INSERT INTO records VALUES(11,1),(12,1),(22,2)"); err != nil {
		t.Fatal(err)
	}
	required := true
	component := &spec.Component{
		Key:        spec.Key{Kind: spec.KindComponent, Name: "Records", Scope: "example/public"},
		Routes:     []*spec.Route{{Method: "GET", Path: "/api/records"}},
		Settings:   &spec.Settings{Cache: &spec.CacheSettings{Enabled: true, Name: "records", Location: t.TempDir(), TTL: "1m", Warmup: &spec.CacheWarmupSettings{Cases: []*spec.CacheWarmupCase{{Set: []*spec.CacheWarmupParam{{Name: "Tenant", Values: []string{"1", "2"}}}}}}}},
		Parameters: []*spec.Parameter{{Name: "Tenant", TypeExpr: "int", Required: &required, Source: spec.BindSource{Kind: "query", Name: "tenant"}}, {Name: "Rows", Source: spec.BindSource{Kind: "output", Name: "view"}}},
		RootView:   &spec.View{Name: "records", Source: &spec.ViewSource{SQL: "SELECT id FROM records WHERE tenant=:Tenant ORDER BY id"}},
	}
	return &configFixture{db: db, component: component}
}

func (f *configFixture) runtime(t *testing.T, options ...druntime.Option) *druntime.Runtime {
	t.Helper()
	artifact, err := bootstrap.BuildArtifact(bootstrap.ArtifactInput{Component: f.component, InputType: reflect.TypeFor[configInput](), OutputType: reflect.TypeFor[configOutput](), DirectViewField: "Rows"})
	if err != nil {
		t.Fatal(err)
	}
	reader, err := artifact.ReaderCompilation().NewExecution(bootstrap.ReaderRuntimeConfig{SQL: &dsql.SQLComponent{DB: f.db.DB}, Aerospike: f.pool})
	if err != nil {
		t.Fatal(err)
	}
	rt, err := druntime.NewRuntime([]*druntime.RegisteredComponent{{Component: artifact.Component, Input: artifact.Input, OutputType: reflect.TypeFor[configOutput](), Reader: reader}}, options...)
	if err != nil {
		t.Fatal(err)
	}
	return rt
}

func warmupConfig() Config {
	return Config{APIPrefix: "/api", Meta: Meta{CacheWarmURI: "/admin/warm"}, Warmup: &WarmupConfig{Lifetime: NewWarmupLifetime(context.Background()), AdminHeaders: []string{"X-Admin"}, Timeout: time.Minute, Completed: func(WarmupResult, error) {}, Authorize: func(_ context.Context, req *stdhttp.Request, target dexec.ComponentTarget) error {
		if req.Header.Get("X-Admin") != "admin" || target.Route.Path != "/api/records" {
			return errors.New("denied")
		}
		return nil
	}}}
}

func TestHTTPWarmupSQLite(t *testing.T) {
	for _, tc := range []struct {
		name   string
		max    int
		values []string
		want   int
		status int
	}{
		{"all declared cases", 0, []string{"1", "2"}, 2, 200},
		{"case budget", 1, []string{"1", "2"}, 1, 200},
		{"invalid input", 0, []string{"bad"}, 0, 500},
	} {
		t.Run(tc.name, func(t *testing.T) {
			f := newConfigFixture(t)
			f.component.Settings.Cache.Warmup.MaxCases = &tc.max
			f.component.Settings.Cache.Warmup.Cases[0].Set[0].Values = tc.values
			rt := f.runtime(t)
			h, err := warmupConfig().NewHandler(rt, nil, "test")
			if err != nil {
				t.Fatal(err)
			}
			defer h.Shutdown(context.Background())
			req := httptest.NewRequest("POST", "/admin/warm/records?tenant=999&target=unknown", strings.NewReader(`{"target":"unknown","tenant":999}`))
			req.Header.Set("X-Admin", "admin")
			response := httptest.NewRecorder()
			h.ServeHTTP(response, req)
			if response.Code != tc.status {
				t.Fatalf("status=%d body=%s", response.Code, response.Body.String())
			}
			var result WarmupResult
			if err := json.Unmarshal(response.Body.Bytes(), &result); err != nil {
				t.Fatal(err)
			}
			if result.Groups != tc.want || result.Target != "GET:/api/records" {
				t.Fatalf("result=%+v", result)
			}
			if tc.status != 200 {
				return
			}
			if err := f.db.ExecStatements(context.Background(), "DROP TABLE records"); err != nil {
				t.Fatal(err)
			}
			for tenant := 1; tenant <= 2; tenant++ {
				req := httptest.NewRequest("GET", []string{"", "/api/records?tenant=1", "/api/records?tenant=2"}[tenant], nil)
				response := httptest.NewRecorder()
				h.ServeHTTP(response, req)
				if tenant <= tc.want {
					if response.Code != 200 || !strings.Contains(response.Body.String(), []string{"", "11", "22"}[tenant]) {
						t.Fatalf("cache not reused: %d %s", response.Code, response.Body.String())
					}
				} else if response.Code == 200 {
					t.Fatal("unwarmed case unexpectedly cached")
				}
			}
		})
	}
}

func TestHTTPWarmupRoutePolicy(t *testing.T) {
	f := newConfigFixture(t)
	f.component.Routes[0].APIKeyHeader = "X-Key"
	f.component.Routes[0].APIKeyValue = "key"
	rt := f.runtime(t)
	for _, tc := range []struct {
		name, method, path, admin, key string
		disabled                       bool
		status                         int
	}{
		{"disabled", "POST", "/admin/warm/records", "admin", "key", true, 404},
		{"unknown", "POST", "/admin/warm/unknown", "admin", "key", false, 404},
		{"wrong method", "GET", "/admin/warm/records", "admin", "key", false, 405},
		{"admin denied", "POST", "/admin/warm/records", "", "key", false, 403},
		{"component denied", "POST", "/admin/warm/records", "admin", "", false, 403},
		{"accepted", "POST", "/admin/warm/records", "admin", "key", false, 200},
	} {
		t.Run(tc.name, func(t *testing.T) {
			config := warmupConfig()
			if tc.disabled {
				config.Meta.CacheWarmURI = " "
			}
			h, err := config.NewHandler(rt, nil, "test")
			if err != nil {
				t.Fatal(err)
			}
			defer h.Shutdown(context.Background())
			req := httptest.NewRequest(tc.method, tc.path, nil)
			req.Header.Set("X-Admin", tc.admin)
			req.Header.Set("X-Key", tc.key)
			res := httptest.NewRecorder()
			h.ServeHTTP(res, req)
			if res.Code != tc.status {
				t.Fatalf("%d %s", res.Code, res.Body.String())
			}
		})
	}
}

func TestHTTPCORSControlsSQLite(t *testing.T) {
	origins := []string{"https://allowed.example"}
	methods := []string{"GET"}
	headers := []string{"X-Token"}
	expose := []string{"X-Result"}
	yes := true
	age := int64(321)
	for _, tc := range []struct {
		name, method, origin, requested, requestedHeaders string
		status                                            int
		allowed                                           bool
	}{
		{"normal", "GET", "https://allowed.example", "", "", 200, true},
		{"normal other origin", "GET", "https://denied.example", "", "", 200, false},
		{"no origin", "GET", "", "", "", 200, false},
		{"preflight", "OPTIONS", "https://allowed.example", "GET", "x-token", 204, true},
		{"denied header", "OPTIONS", "https://allowed.example", "GET", "X-Other", 403, false},
		{"denied origin", "OPTIONS", "https://denied.example", "GET", "X-Token", 403, false},
		{"unknown method", "OPTIONS", "https://allowed.example", "DELETE", "", 405, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			f := newConfigFixture(t)
			h, err := (Config{Meta: Meta{CacheWarmURI: " "}, CORS: &spec.CORS{AllowOrigins: &origins, AllowMethods: &methods, AllowHeaders: &headers, ExposeHeaders: &expose, AllowCredentials: &yes, MaxAge: &age}}).NewHandler(f.runtime(t), nil, "test")
			if err != nil {
				t.Fatal(err)
			}
			req := httptest.NewRequest(tc.method, "/api/records?tenant=1", nil)
			req.Header.Set("Origin", tc.origin)
			req.Header.Set("Access-Control-Request-Method", tc.requested)
			req.Header.Set("Access-Control-Request-Headers", tc.requestedHeaders)
			res := httptest.NewRecorder()
			h.ServeHTTP(res, req)
			if res.Code != tc.status || (res.Header().Get("Access-Control-Allow-Origin") != "") != tc.allowed {
				t.Fatalf("status=%d headers=%v body=%s", res.Code, res.Header(), res.Body.String())
			}
			if tc.allowed {
				if res.Header().Get("Access-Control-Allow-Credentials") != "true" {
					t.Fatal("credentials lost")
				}
				if tc.method == "OPTIONS" && (res.Header().Get("Access-Control-Max-Age") != "321" || res.Header().Get("Access-Control-Allow-Methods") != "GET") {
					t.Fatal(res.Header())
				}
				if tc.method == "GET" && res.Header().Get("Access-Control-Expose-Headers") != "X-Result" {
					t.Fatal(res.Header())
				}
			}
		})
	}
}

func TestHTTPCORSInheritanceAndIsolation(t *testing.T) {
	f := newConfigFixture(t)
	origins := []string{"https://allowed.example"}
	wildcard := []string{"*"}
	empty := []string{}
	yes, no := true, false
	f.component.Routes = append(f.component.Routes, &spec.Route{Method: "POST", Path: "/api/records", CORS: &spec.CORS{AllowOrigins: &empty, AllowCredentials: &no}}, &spec.Route{Method: "GET", Path: "/api/override", CORS: &spec.CORS{AllowCredentials: &no, AllowHeaders: &empty}})
	rt := f.runtime(t)
	config := Config{Meta: Meta{CacheWarmURI: " "}, CORS: &spec.CORS{AllowOrigins: &origins, AllowMethods: &wildcard, AllowHeaders: &wildcard, AllowCredentials: &yes}}
	h, err := config.NewHandler(rt, nil, "test")
	if err != nil {
		t.Fatal(err)
	}
	origins[0] = "https://mutated.example"
	wildcard[0] = "denied"
	yes = false
	for _, tc := range []struct {
		method, path, headers string
		status                int
		credentials           string
	}{
		{"GET", "/api/records", "X-Any", 204, "true"},
		{"POST", "/api/records", "", 403, ""},
		{"GET", "/api/override", "", 204, ""},
		{"GET", "/api/override", "X-Any", 403, ""},
	} {
		req := httptest.NewRequest("OPTIONS", tc.path, nil)
		req.Header.Set("Origin", "https://allowed.example")
		req.Header.Set("Access-Control-Request-Method", tc.method)
		req.Header.Set("Access-Control-Request-Headers", tc.headers)
		res := httptest.NewRecorder()
		h.ServeHTTP(res, req)
		if res.Code != tc.status || res.Header().Get("Access-Control-Allow-Credentials") != tc.credentials {
			t.Fatalf("%+v: %d %v", tc, res.Code, res.Header())
		}
	}
}

func TestHTTPCORSErrorAndExplicitMethodDenial(t *testing.T) {
	f := newConfigFixture(t)
	wildcard := []string{"*"}
	methods := []string{"POST"}
	f.component.Routes[0].APIKeyHeader = "X-Key"
	f.component.Routes[0].APIKeyValue = "key"
	rt := f.runtime(t)
	for _, tc := range []struct {
		name   string
		policy *spec.CORS
		method string
		status int
		allow  bool
	}{
		{"authorization error", &spec.CORS{AllowOrigins: &wildcard}, "GET", 403, true},
		{"configured method denied", &spec.CORS{AllowOrigins: &wildcard, AllowMethods: &methods}, "OPTIONS", 403, false},
		{"disabled", nil, "OPTIONS", 405, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			h, err := (Config{Meta: Meta{CacheWarmURI: " "}, DisableCors: tc.policy == nil, CORS: tc.policy}).NewHandler(rt, nil, "test")
			if err != nil {
				t.Fatal(err)
			}
			req := httptest.NewRequest(tc.method, "/api/records", nil)
			req.Header.Set("Origin", "https://a.example")
			req.Header.Set("Access-Control-Request-Method", "GET")
			res := httptest.NewRecorder()
			h.ServeHTTP(res, req)
			if res.Code != tc.status || (res.Header().Get("Access-Control-Allow-Origin") != "") != tc.allow {
				t.Fatalf("%d %v", res.Code, res.Header())
			}
		})
	}
}
