package http

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"reflect"
	"strconv"
	"strings"
	"testing"

	"github.com/viant/datly/bootstrap"
	dexec "github.com/viant/datly/exec"
	druntime "github.com/viant/datly/runtime"
	"github.com/viant/datly/runtime/handler/custom"
	"github.com/viant/datly/runtime/registry"
	"github.com/viant/datly/spec"
	"github.com/viant/xdatly/response"
)

type securityLog struct{ failure error }

func (*securityLog) Debug(string, ...any) {}
func (*securityLog) Info(string, ...any)  {}
func (*securityLog) Warn(string, ...any)  {}
func (l *securityLog) Error(_ string, args ...any) {
	for _, v := range args {
		if err, ok := v.(error); ok {
			l.failure = err
		}
	}
}

func TestSecurityHTTPBindingAndDiagnosticsSQLite(t *testing.T) {
	for _, kind := range []string{"query", "path", "header", "cookie", "form", "body"} {
		t.Run(kind, func(t *testing.T) {
			f := newConfigFixture(t)
			f.component.Parameters[0].Source = spec.BindSource{Kind: kind, Name: "tenant"}
			if kind == "path" {
				f.component.Routes[0].Path = "/api/records/{tenant}"
			}
			if kind == "form" {
				f.component.Routes[0].Method = "POST"
			}
			log := &securityLog{}
			h, err := (Config{Meta: Meta{CacheWarmURI: " "}}).NewHandler(f.runtime(t), log, "test")
			if err != nil {
				t.Fatal(err)
			}
			uri := "/api/records?tenant=private-invalid"
			body := ""
			if kind == "path" {
				uri = "/api/records/private-invalid"
			}
			if kind == "form" {
				uri = "/api/records"
				body = "tenant=private-invalid"
			}
			if kind == "body" {
				body = `{"tenant":"private-invalid"}`
			}
			req := httptest.NewRequest("GET", uri, strings.NewReader(body))
			req.Header.Set("tenant", "private-invalid")
			req.AddCookie(&http.Cookie{Name: "tenant", Value: "private-invalid"})
			if kind == "form" {
				req.Method = "POST"
				req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
			}
			if kind == "body" {
				req.Header.Set("Content-Type", "application/json")
			}
			res := httptest.NewRecorder()
			h.ServeHTTP(res, req)
			if res.Code != 400 || strings.Contains(res.Body.String(), "private-invalid") || strings.Contains(res.Body.String(), "strconv") {
				t.Fatalf("%d %s", res.Code, res.Body)
			}
			if log.failure == nil {
				t.Fatal("private binding error not logged")
			}
			if kind != "body" {
				var conversion *strconv.NumError
				if !errors.As(log.failure, &conversion) {
					t.Fatalf("conversion cause lost: %v", log.failure)
				}
			}
		})
	}
}

func TestSecurityHTTPUnknownAndPanicDiscardOutput(t *testing.T) {
	type input struct{}
	type output struct{ Secret string }
	for _, mode := range []string{"unknown", "panic", "panic public error"} {
		t.Run(mode, func(t *testing.T) {
			component := &spec.Component{Key: spec.Key{Kind: spec.KindComponent, Scope: "security", Name: "Fail"}, Routes: []*spec.Route{{Method: "GET", Path: "/fail"}}}
			artifact, err := bootstrap.BuildArtifact(bootstrap.ArtifactInput{Component: component, InputType: reflect.TypeFor[input](), OutputType: reflect.TypeFor[output]()})
			if err != nil {
				t.Fatal(err)
			}
			cause := errors.New("failed to parse PRIVATE panic and driver details")
			rt, err := druntime.NewRuntime([]*registry.RegisteredComponent{{Component: artifact.Component, Input: artifact.Input, OutputType: reflect.TypeFor[output](), Handler: custom.NewFunc[input, output](func(context.Context, *input) (*output, error) {
				if mode == "panic" {
					panic(cause)
				}
				if mode == "panic public error" {
					panic(&response.Error{Code: 400, Payload: "PRIVATE panic payload", Cause: cause})
				}
				return &output{Secret: "PRIVATE partial result"}, cause
			})}})
			if err != nil {
				t.Fatal(err)
			}
			log := &securityLog{}
			res := httptest.NewRecorder()
			NewHandler(rt, log, "test").ServeHTTP(res, httptest.NewRequest("GET", "/fail", nil))
			if res.Code != 500 || strings.Contains(res.Body.String(), "PRIVATE") || !strings.Contains(res.Body.String(), "Internal Server Error") {
				t.Fatalf("%d %s", res.Code, res.Body)
			}
			if log.failure == nil {
				t.Fatal("diagnostic lost")
			}
			if mode != "unknown" {
				var p *dexec.PanicError
				if !errors.As(log.failure, &p) || len(p.Stack()) == 0 || p.Cause() == nil {
					t.Fatalf("missing typed panic: %v", log.failure)
				}
			} else if !errors.Is(log.failure, cause) {
				t.Fatal("cause lost")
			}
		})
	}
}

func TestSecurityMetricsPolicySQLite(t *testing.T) {
	for _, tc := range []struct {
		name         string
		policy       *MetricsConfig
		mode         string
		headers, sql bool
	}{
		{"default", nil, "debug", false, false},
		{"redacted", &MetricsConfig{}, "debug", true, false},
		{"authorized debug", &MetricsConfig{AllowSQL: true}, "debug", true, true},
		{"normal with SQL policy", &MetricsConfig{AllowSQL: true}, "true", true, false},
		{"denied", &MetricsConfig{AllowSQL: true, Authorize: func(*http.Request) error { return errors.New("denied") }}, "debug", false, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			f := newConfigFixture(t)
			h, err := (Config{Meta: Meta{CacheWarmURI: " "}, Metrics: tc.policy}).NewHandler(f.runtime(t), nil, "test")
			if err != nil {
				t.Fatal(err)
			}
			req := httptest.NewRequest("GET", "/api/records?tenant=1", nil)
			req.Header.Set(datlyRequestMetricsHeader, tc.mode)
			res := httptest.NewRecorder()
			h.ServeHTTP(res, req)
			metrics := ""
			for name, values := range res.Header() {
				if strings.HasPrefix(name, "Datly-Metrics-") {
					metrics += strings.Join(values, "")
				}
			}
			if res.Code != 200 || (metrics != "") != tc.headers || strings.Contains(metrics, `"sql":`) != tc.sql || strings.Contains(metrics, `"args":`) != tc.sql {
				t.Fatalf("status=%d metrics=%s", res.Code, metrics)
			}
		})
	}
}

func TestSecurityCORSDefaultsSQLite(t *testing.T) {
	yes := true
	wild := []string{"*"}
	trusted := []string{"https://trusted.example"}
	empty := []string{}
	for _, tc := range []struct {
		name                          string
		policy                        *spec.CORS
		disable, invalid, credentials bool
	}{
		{name: "default"}, {name: "disabled", disable: true}, {name: "empty", policy: &spec.CORS{}},
		{name: "trusted", policy: &spec.CORS{AllowOrigins: &trusted, AllowCredentials: &yes}, credentials: true},
		{name: "wildcard credential rejected", policy: &spec.CORS{AllowOrigins: &wild, AllowCredentials: &yes}, invalid: true},
		{name: "empty origins", policy: &spec.CORS{AllowOrigins: &empty, AllowCredentials: &yes}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			f := newConfigFixture(t)
			h, err := (Config{Meta: Meta{CacheWarmURI: " "}, CORS: tc.policy, DisableCors: tc.disable}).NewHandler(f.runtime(t), nil, "test")
			if tc.invalid {
				if err == nil {
					t.Fatal("unsafe policy accepted")
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			for _, origin := range []string{"https://foreign.example", "https://trusted.example"} {
				for _, method := range []string{"GET", "OPTIONS"} {
					req := httptest.NewRequest(method, "/api/records?tenant=1", nil)
					req.Header.Set("Origin", origin)
					if method == "OPTIONS" {
						req.Header.Set("Access-Control-Request-Method", "GET")
					}
					res := httptest.NewRecorder()
					h.ServeHTTP(res, req)
					want := tc.credentials && origin == trusted[0]
					if (res.Header().Get("Access-Control-Allow-Credentials") == "true") != want {
						t.Fatalf("%s %s %s: %v", tc.name, method, origin, res.Header())
					}
				}
			}
		})
	}
}

func TestSecurityAPIKeyOwner(t *testing.T) {
	for _, v := range []string{"secret", "", "secreu", "short", "secret-long"} {
		if got := (APIKey{Value: "secret"}).matchesValue(v); got != (v == "secret") {
			t.Fatal(fmt.Sprint(v, got))
		}
	}
	if (APIKey{}).matchesValue("") {
		t.Fatal("empty key authorized")
	}
}

func TestSecurityHTTPMalformedBodySQLite(t *testing.T) {
	for _, tc := range []struct {
		name, media, body string
		status            int
	}{
		{"syntax", "application/json", `{"tenant":`, 400},
		{"wrong type", "application/json", `{"tenant":"private"}`, 400},
		{"invalid media", "application/json;broken", `{"tenant":1}`, 400},
		{"unsupported named media", "application/xml", `<tenant>1</tenant>`, 415},
	} {
		t.Run(tc.name, func(t *testing.T) {
			f := newConfigFixture(t)
			f.component.Parameters[0].Source = spec.BindSource{Kind: "body", Name: "tenant"}
			h, err := (Config{Meta: Meta{CacheWarmURI: " "}}).NewHandler(f.runtime(t), nil, "test")
			if err != nil {
				t.Fatal(err)
			}
			req := httptest.NewRequest("GET", "/api/records", strings.NewReader(tc.body))
			req.Header.Set("Content-Type", tc.media)
			res := httptest.NewRecorder()
			h.ServeHTTP(res, req)
			if res.Code != tc.status || strings.Contains(res.Body.String(), "private") || strings.Contains(res.Body.String(), "json:") {
				t.Fatalf("%d %s", res.Code, res.Body)
			}
		})
	}
}
