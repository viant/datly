package config_test

import (
	"context"
	"github.com/viant/datly/standalone/config"
	"os"
	"path/filepath"
	"testing"
)

func TestSecurityTimeoutAndMetricsConfiguration(t *testing.T) {
	for _, tc := range []struct{ name, input string }{
		{"json", `{"GoBootstrap":{"Packages":["example.com/app"]},"Endpoint":{"ReadHeaderTimeoutMs":1200000,"IdleTimeoutMs":-1,"WriteTimeoutMs":3600000},"Metrics":{"AllowSQL":true}}`},
		{"yaml", "GoBootstrap:\n  Packages: [example.com/app]\nEndpoint:\n  ReadHeaderTimeoutMs: 1200000\n  IdleTimeoutMs: -1\n  WriteTimeoutMs: 3600000\nMetrics:\n  AllowSQL: true\n"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "config."+tc.name)
			if err := os.WriteFile(path, []byte(tc.input), 0600); err != nil {
				t.Fatal(err)
			}
			c, err := (config.Loader{}).Load(context.Background(), path)
			if err != nil {
				t.Fatal(err)
			}
			if c.Endpoint.ReadHeaderTimeoutMs != 1200000 || c.Endpoint.IdleTimeoutMs != -1 || c.Endpoint.WriteTimeoutMs != 3600000 || c.Metrics == nil || !c.Metrics.AllowSQL {
				t.Fatalf("lost policy: %+v", c)
			}
		})
	}
	for _, endpoint := range []config.Endpoint{{ReadHeaderTimeoutMs: int(^uint(0) >> 1)}, {IdleTimeoutMs: int(^uint(0) >> 1)}} {
		if _, err := endpoint.ListenAddress(); err == nil {
			t.Fatal("overflow accepted")
		}
	}
}
