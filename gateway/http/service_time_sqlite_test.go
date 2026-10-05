package http

import (
	"encoding/json"
	"net/http/httptest"
	"testing"
	"time"

	"gopkg.in/yaml.v3"
)

func TestServiceTimePolicySQLite(t *testing.T) {
	for _, name := range []string{"", "Datly-Service-Time", "X-Execution-Time"} {
		t.Run("header="+name, func(t *testing.T) {
			f := newConfigFixture(t)
			h, err := (Config{ServiceTimeHeader: name, Meta: Meta{CacheWarmURI: " "}}).NewHandler(f.runtime(t), nil, "test")
			if err != nil {
				t.Fatal(err)
			}
			for _, path := range []string{"/api/records?tenant=1", "/api/records?tenant=invalid", "/unknown"} {
				res := httptest.NewRecorder()
				h.ServeHTTP(res, httptest.NewRequest("GET", path, nil))
				if path == "/api/records?tenant=1" && res.Code != 200 {
					t.Fatalf("success status=%d", res.Code)
				}
				if path != "/api/records?tenant=1" && res.Code < 400 {
					t.Fatalf("error status=%d", res.Code)
				}
				if name == "" || path == "/unknown" {
					if res.Header().Get("Datly-Service-Time") != "" || res.Header().Get("X-Execution-Time") != "" {
						t.Fatalf("unexpected timing: %v", res.Header())
					}
				} else {
					value, err := time.ParseDuration(res.Header().Get(name))
					if err != nil || value < 0 {
						t.Fatalf("invalid timing %q: %v", res.Header().Get(name), err)
					}
					if name != "Datly-Service-Time" && res.Header().Get("Datly-Service-Time") != "" {
						t.Fatal("rename emitted both names")
					}
				}
			}
		})
	}
	f := newConfigFixture(t)
	res := httptest.NewRecorder()
	NewHandler(f.runtime(t), nil, "test").ServeHTTP(res, httptest.NewRequest("GET", "/api/records?tenant=1", nil))
	if res.Code != 200 || res.Header().Get("Datly-Service-Time") != "" {
		t.Fatalf("direct default: %d %v", res.Code, res.Header())
	}
}

func TestServiceTimeConfigurationSQLite(t *testing.T) {
	f := newConfigFixture(t)
	rt := f.runtime(t)
	for _, name := range []string{" ", "X Time", "X:Time", "X-Time\r\nInjected", "時刻"} {
		if _, err := (Config{ServiceTimeHeader: name}).NewHandler(rt, nil, "test"); err == nil {
			t.Errorf("accepted invalid name %q", name)
		}
	}
	for _, decoder := range []struct {
		name   string
		decode func([]byte, any) error
		data   string
	}{
		{"json", json.Unmarshal, `{"ServiceTimeHeader":"X-Execution-Time"}`},
		{"yaml", yaml.Unmarshal, "ServiceTimeHeader: X-Execution-Time\n"},
	} {
		t.Run(decoder.name, func(t *testing.T) {
			var c Config
			if err := decoder.decode([]byte(decoder.data), &c); err != nil {
				t.Fatal(err)
			}
			c.Meta.CacheWarmURI = " "
			h, err := c.NewHandler(rt, nil, "test")
			if err != nil {
				t.Fatal(err)
			}
			res := httptest.NewRecorder()
			h.ServeHTTP(res, httptest.NewRequest("GET", "/api/records?tenant=1", nil))
			if _, err = time.ParseDuration(res.Header().Get("X-Execution-Time")); err != nil {
				t.Fatal(err)
			}
		})
	}
}
