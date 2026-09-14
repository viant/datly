package http

import (
	"github.com/viant/datly/spec"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
)

func TestCORSFinalHeadersPreserveConfiguredPolicy(t *testing.T) {
	origins := []string{"https://allowed.example"}
	yes := true
	policy, err := newCORSPolicy(&spec.CORS{AllowOrigins: &origins, AllowCredentials: &yes})
	if err != nil {
		t.Fatal(err)
	}
	for _, origin := range []string{"https://allowed.example", "https://denied.example"} {
		res := httptest.NewRecorder()
		req := httptest.NewRequest("GET", "/records", nil)
		req.Header.Set("Origin", origin)
		writer := &corsResponseWriter{ResponseWriter: res, policy: policy, request: req, method: "GET"}
		writer.Header().Set("Access-Control-Allow-Origin", "*")
		writer.Header().Set("Access-Control-Allow-Credentials", "true")
		writer.Header().Set("Vary", "Accept-Encoding")
		writer.WriteHeader(500)
		if !strings.Contains(strings.Join(res.Header().Values("Vary"), ","), "Origin") {
			t.Fatal("response lost Origin variance")
		}
		if origin == origins[0] {
			if res.Header().Get("Access-Control-Allow-Origin") != origin {
				t.Fatal(res.Header())
			}
		} else if res.Header().Get("Access-Control-Allow-Origin") != "" || res.Header().Get("Access-Control-Allow-Credentials") != "" {
			t.Fatal("application response widened CORS")
		}
	}
}

func TestCORSExistingConstructorAndOptions(t *testing.T) {
	f := newConfigFixture(t)
	origins := []string{"*"}
	f.component.Routes[0].CORS = &spec.CORS{AllowOrigins: &origins}
	h := NewHandler(f.runtime(t), nil, "test")
	origins[0] = "changed"
	req := httptest.NewRequest("GET", "/api/records?tenant=1", nil)
	req.Header.Set("Origin", "https://allowed.example")
	res := httptest.NewRecorder()
	h.ServeHTTP(res, req)
	if res.Code != 200 || res.Header().Get("Access-Control-Allow-Origin") != "https://allowed.example" {
		t.Fatal(res.Header())
	}
	options := httptest.NewRecorder()
	h.ServeHTTP(options, httptest.NewRequest("OPTIONS", "/api/records", nil))
	if options.Code != 204 || options.Header().Get("Allow") != "GET, OPTIONS" || options.Header().Get("Access-Control-Allow-Origin") != "" {
		t.Fatalf("%d %v", options.Code, options.Header())
	}
}

func TestCORSConcurrentRoutePolicies(t *testing.T) {
	f := newConfigFixture(t)
	getOrigins := []string{"https://get.example"}
	postOrigins := []string{"https://post.example"}
	f.component.Routes[0].CORS = &spec.CORS{AllowOrigins: &getOrigins}
	f.component.Routes = append(f.component.Routes, &spec.Route{Method: "POST", Path: "/api/records", CORS: &spec.CORS{AllowOrigins: &postOrigins}})
	h := NewHandler(f.runtime(t), nil, "test")
	var active sync.WaitGroup
	for index := 0; index < 20; index++ {
		active.Add(1)
		go func(index int) {
			defer active.Done()
			method, origin := "GET", "https://get.example"
			if index%2 != 0 {
				method, origin = "POST", "https://post.example"
			}
			req := httptest.NewRequest("OPTIONS", "/api/records", nil)
			req.Header.Set("Origin", origin)
			req.Header.Set("Access-Control-Request-Method", method)
			res := httptest.NewRecorder()
			h.ServeHTTP(res, req)
			if res.Code != 204 || res.Header().Get("Access-Control-Allow-Origin") != origin || res.Header().Get("Access-Control-Allow-Methods") != method {
				t.Errorf("policy leaked: %d %v", res.Code, res.Header())
			}
		}(index)
	}
	active.Wait()
}
