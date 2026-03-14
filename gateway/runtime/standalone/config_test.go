package standalone

import (
	"testing"

	"github.com/viant/datly/gateway"
)

func TestConfigNormalizeURLs_LeavesEmptyRouteAndPluginURLsUntouched(t *testing.T) {
	cfg := &Config{Config: &gateway.Config{}}
	cfg.normalizeURLs("/tmp")
	if cfg.RouteURL != "" {
		t.Fatalf("RouteURL = %q, want empty", cfg.RouteURL)
	}
	if cfg.PluginsURL != "" {
		t.Fatalf("PluginsURL = %q, want empty", cfg.PluginsURL)
	}
	if cfg.ContentURL != "" {
		t.Fatalf("ContentURL = %q, want empty", cfg.ContentURL)
	}
}
