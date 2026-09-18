package http

import (
	"context"
	"encoding/json"
	"net/http/httptest"
	"testing"
)

func TestHTTPWarmupNotFoundMessage(t *testing.T) {
	for _, tc := range []struct {
		name, path, message string
		noTargets, disabled bool
	}{
		{"unknown target", "/admin/warm/unknown", "No warmup target is configured for this route.", false, false},
		{"no configured targets", "/admin/warm/records", "No warmup target is configured for this route.", true, false},
		{"namespace root", "/admin/warm", "No warmup target is configured for this route.", true, false},
		{"unrelated missing route", "/api/missing", "not found", true, false},
		{"prefix boundary", "/admin/warmer/records", "not found", true, false},
		{"disabled namespace", "/admin/warm/records", "not found", false, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			f := newConfigFixture(t)
			if tc.noTargets {
				f.component.Settings.Cache.Warmup = nil
			}
			config := warmupConfig()
			if tc.disabled {
				config.Meta.CacheWarmURI = " "
			}
			config.Warmup.Authorize = nil
			if !tc.noTargets && !tc.disabled {
				config.Warmup.Authorize = warmupConfig().Warmup.Authorize
			}
			config.Warmup.Completed = func(WarmupResult, error) { t.Error("unexpected warmup execution") }
			h, err := config.NewHandler(f.runtime(t), nil, "test")
			if err != nil {
				t.Fatal(err)
			}
			defer h.Shutdown(context.Background())
			res := httptest.NewRecorder()
			h.ServeHTTP(res, httptest.NewRequest("POST", tc.path, nil))
			if res.Code != 404 || res.Header().Get("Content-Type") != "application/json" {
				t.Fatalf("status=%d headers=%v body=%s", res.Code, res.Header(), res.Body.String())
			}
			var body map[string]string
			if err := json.Unmarshal(res.Body.Bytes(), &body); err != nil {
				t.Fatal(err)
			}
			if body["status"] != "error" || body["message"] != tc.message {
				t.Fatalf("unexpected error response: %v", body)
			}
		})
	}
}
