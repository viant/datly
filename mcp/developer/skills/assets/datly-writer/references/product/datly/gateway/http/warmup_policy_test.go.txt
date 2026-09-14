package http

import (
	"context"
	"encoding/json"
	"fmt"
	dexec "github.com/viant/datly/exec"
	druntime "github.com/viant/datly/runtime"
	"github.com/viant/datly/spec"
	stdhttp "net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestHTTPWarmupEligibilityAndConfig(t *testing.T) {
	for _, mode := range []string{"private", "no settings", "cache disabled", "not GET"} {
		t.Run(mode, func(t *testing.T) {
			f := newConfigFixture(t)
			var options []druntime.Option
			switch mode {
			case "private":
				options = append(options, druntime.WithExposedPackages([]string{"example/other"}, nil))
			case "no settings":
				f.component.Settings.Cache.Warmup = nil
			case "cache disabled":
				f.component.Settings.Cache = nil
			case "not GET":
				f.component.Routes[0].Method = "POST"
			}
			h, err := warmupConfig().NewHandler(f.runtime(t, options...), nil, "test")
			if err != nil {
				t.Fatal(err)
			}
			defer h.Shutdown(context.Background())
			req := httptest.NewRequest("POST", "/admin/warm/records", nil)
			req.Header.Set("X-Admin", "admin")
			res := httptest.NewRecorder()
			h.ServeHTTP(res, req)
			if res.Code != 404 {
				t.Fatalf("ineligible component exposed: %d", res.Code)
			}
		})
	}
	f := newConfigFixture(t)
	rt := f.runtime(t)
	for _, mode := range []string{"no admin", "no context", "no timeout", "no outcome", "invalid prefix", "collision"} {
		t.Run(mode, func(t *testing.T) {
			config := warmupConfig()
			switch mode {
			case "no admin":
				config.Warmup.Authorize = nil
			case "no context":
				config.Warmup.Lifetime = nil
			case "no timeout":
				config.Warmup.Timeout = 0
			case "no outcome":
				config.Warmup.Completed = nil
			case "invalid prefix":
				config.Meta.CacheWarmURI = "/warm?target="
			case "collision":
				config.Meta.CacheWarmURI = "/api"
			}
			if h, err := config.NewHandler(rt, nil, "test"); err == nil {
				h.Shutdown(context.Background())
				t.Fatal("invalid config accepted")
			}
		})
	}
}

func TestHTTPWarmupIndexedSQLite(t *testing.T) {
	f := newConfigFixture(t)
	optional := false
	f.component.Parameters[0].Required = &optional
	f.component.RootView.Source.SQL = "SELECT id FROM records WHERE (:Tenant=0 OR id=:Tenant) ORDER BY id"
	f.component.Settings.Cache.Warmup = &spec.CacheWarmupSettings{IndexColumn: "id", IndexParameter: "Tenant"}
	h, err := warmupConfig().NewHandler(f.runtime(t), nil, "test")
	if err != nil {
		t.Fatal(err)
	}
	defer h.Shutdown(context.Background())
	req := httptest.NewRequest("POST", "/admin/warm/records?tenant=22", nil)
	req.Header.Set("X-Admin", "admin")
	res := httptest.NewRecorder()
	h.ServeHTTP(res, req)
	if res.Code != 200 {
		t.Fatalf("%d %s", res.Code, res.Body.String())
	}
	if err := f.db.ExecStatements(context.Background(), "DROP TABLE records"); err != nil {
		t.Fatal(err)
	}
	for _, query := range []string{"11", "12", "22", "999"} {
		res := httptest.NewRecorder()
		h.ServeHTTP(res, httptest.NewRequest("GET", "/api/records?tenant="+query, nil))
		if res.Code != 200 {
			t.Fatalf("indexed cache not reused for %s: %d %s", query, res.Code, res.Body.String())
		}
	}
}

func TestHTTPWarmupCompletionAfterCancellation(t *testing.T) {
	f := newConfigFixture(t)
	rt, gate := f.gated(t)
	config := warmupConfig()
	var result WarmupResult
	var outcome error
	config.Warmup.Completed = func(actual WarmupResult, err error) { result = actual; outcome = err }
	h, err := config.NewHandler(rt, nil, "test")
	if err != nil {
		t.Fatal(err)
	}
	defer h.Shutdown(context.Background())
	req := httptest.NewRequest("POST", "/admin/warm/records", nil)
	req.Header.Set("X-Admin", "admin")
	res := httptest.NewRecorder()
	done := make(chan struct{})
	go func() { h.ServeHTTP(res, req); close(done) }()
	<-gate.started
	if err := h.Shutdown(context.Background()); err != nil {
		t.Fatal(err)
	}
	<-done
	if outcome == nil || result.Status != "error" || result.Groups != 0 {
		t.Fatalf("lost outcome %+v %v", result, outcome)
	}
}

func TestHTTPWarmupDeclaredLimitSQLite(t *testing.T) {
	f := newConfigFixture(t)
	limit := 1
	f.component.Settings.Cache.Warmup.Limit = &limit
	f.component.Settings.Cache.Warmup.Cases[0].Set[0].Values = []string{"1"}
	// The normal query declares the same limit; SQLX owns its cache identity.
	f.component.RootView.Source.Controls = &spec.ViewControls{Limit: &limit}
	h, err := warmupConfig().NewHandler(f.runtime(t), nil, "test")
	if err != nil {
		t.Fatal(err)
	}
	defer h.Shutdown(context.Background())
	req := httptest.NewRequest("POST", "/admin/warm/records?limit=999", nil)
	req.Header.Set("X-Admin", "admin")
	res := httptest.NewRecorder()
	h.ServeHTTP(res, req)
	if res.Code != 200 {
		t.Fatalf("%d %s", res.Code, res.Body.String())
	}
	if err := f.db.ExecStatements(context.Background(), "DROP TABLE records"); err != nil {
		t.Fatal(err)
	}
	cached := httptest.NewRecorder()
	h.ServeHTTP(cached, httptest.NewRequest("GET", "/api/records?tenant=1", nil))
	if cached.Code != 200 {
		t.Fatalf("limited cache not reused: %s", cached.Body.String())
	}
	if strings.Contains(cached.Body.String(), "12") || !strings.Contains(cached.Body.String(), "11") {
		t.Fatalf("limit changed: %s", cached.Body.String())
	}
}

func TestHTTPWarmupCaseDefaultsSQLite(t *testing.T) {
	for _, exclude := range []bool{false, true} {
		t.Run(fmt.Sprint(exclude), func(t *testing.T) {
			f := newConfigFixture(t)
			optional := false
			f.component.Parameters[0].Required = &optional
			f.component.RootView.Source.SQL = "SELECT id FROM records WHERE (:Tenant=0 OR tenant=:Tenant) ORDER BY id"
			f.component.Settings.Cache.Warmup.Cases[0].Set[0].Values = []string{"1"}
			f.component.Settings.Cache.Warmup.Cases[0].Set[0].ExcludeDefault = exclude
			h, err := warmupConfig().NewHandler(f.runtime(t), nil, "test")
			if err != nil {
				t.Fatal(err)
			}
			defer h.Shutdown(context.Background())
			req := httptest.NewRequest("POST", "/admin/warm/records", nil)
			req.Header.Set("X-Admin", "admin")
			res := httptest.NewRecorder()
			h.ServeHTTP(res, req)
			if res.Code != 200 {
				t.Fatalf("%d %s", res.Code, res.Body.String())
			}
			var result WarmupResult
			if err := json.Unmarshal(res.Body.Bytes(), &result); err != nil {
				t.Fatal(err)
			}
			want := 2
			if exclude {
				want = 1
			}
			if result.Groups != want {
				t.Fatalf("case count %+v", result)
			}
			if err := f.db.ExecStatements(context.Background(), "DROP TABLE records"); err != nil {
				t.Fatal(err)
			}
			cached := httptest.NewRecorder()
			h.ServeHTTP(cached, httptest.NewRequest("GET", "/api/records", nil))
			if (cached.Code == 200) == exclude {
				t.Fatalf("default case policy changed: %d %s", cached.Code, cached.Body.String())
			}
		})
	}
}

func TestHTTPWarmupPartialOutcomeSQLite(t *testing.T) {
	f := newConfigFixture(t)
	rt, gate := f.gated(t)
	gate.after = fmt.Errorf("failure after native cache write")
	close(gate.release)
	h, err := warmupConfig().NewHandler(rt, nil, "test")
	if err != nil {
		t.Fatal(err)
	}
	defer h.Shutdown(context.Background())
	req := httptest.NewRequest("POST", "/admin/warm/records", nil)
	req.Header.Set("X-Admin", "admin")
	res := httptest.NewRecorder()
	h.ServeHTTP(res, req)
	var result WarmupResult
	if err := json.Unmarshal(res.Body.Bytes(), &result); err != nil {
		t.Fatal(err)
	}
	if res.Code != 500 || result.Status != "error" || result.Groups != 1 {
		t.Fatalf("lost partial native outcome: %d %+v", res.Code, result)
	}
	if err := f.db.ExecStatements(context.Background(), "DROP TABLE records"); err != nil {
		t.Fatal(err)
	}
	cached := httptest.NewRecorder()
	h.ServeHTTP(cached, httptest.NewRequest("GET", "/api/records?tenant=1", nil))
	if cached.Code != 200 {
		t.Fatalf("native cache write proof missing: %s", cached.Body.String())
	}
}

func TestHTTPWarmupTemplateAndPreflight(t *testing.T) {
	f := newConfigFixture(t)
	f.component.Routes[0].Path = "/api/records/{id}"
	config := warmupConfig()
	config.Warmup.Authorize = func(_ context.Context, req *stdhttp.Request, target dexec.ComponentTarget) error {
		if req.Header.Get("X-Admin") != "admin" || target.Route.Path != "/api/records/{id}" {
			return fmt.Errorf("wrong target")
		}
		return nil
	}
	wildcard := []string{"*"}
	methods := []string{"POST"}
	config.CORS = &spec.CORS{AllowOrigins: &wildcard, AllowMethods: &methods, AllowHeaders: &wildcard}
	h, err := config.NewHandler(f.runtime(t), nil, "test")
	if err != nil {
		t.Fatal(err)
	}
	defer h.Shutdown(context.Background())
	for _, tc := range []struct {
		method, requested string
		status            int
	}{
		{"OPTIONS", "POST", 204}, {"OPTIONS", "GET", 403}, {"POST", "", 200},
	} {
		req := httptest.NewRequest(tc.method, "/admin/warm/records/999", nil)
		req.Header.Set("Origin", "https://allowed.example")
		req.Header.Set("Access-Control-Request-Method", tc.requested)
		req.Header.Set("Access-Control-Request-Headers", "X-Admin")
		if tc.method == "POST" {
			req.Header.Set("X-Admin", "admin")
		}
		res := httptest.NewRecorder()
		h.ServeHTTP(res, req)
		if res.Code != tc.status {
			t.Fatalf("%+v: %d %s", tc, res.Code, res.Body.String())
		}
	}
}
