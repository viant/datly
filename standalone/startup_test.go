package standalone

import (
	"context"
	"errors"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"

	mcpserver "github.com/viant/datly/mcp/server"
	"github.com/viant/datly/spec"
	"github.com/viant/datly/standalone/config"
	fixture "github.com/viant/datly/standalone/testdata/app"
	"github.com/viant/datly/standalone/testdata/app/records"
	"github.com/viant/mcp-protocol/authorization"
	oauthmeta "github.com/viant/mcp-protocol/oauth2/meta"
)

func TestSourceStartupFailuresAndCleanup(t *testing.T) {
	for _, tc := range []struct {
		name, match string
		change      func(*testing.T, *fixture.Fixture, *config.Config)
	}{
		{"missing package", "no authored components", func(t *testing.T, f *fixture.Fixture, c *config.Config) {
			c.GoBootstrap.Packages = []string{fixture.Module + "/absent"}
		}},
		{"missing resource", "read.sql", func(t *testing.T, f *fixture.Fixture, c *config.Config) {
			if err := os.Remove(filepath.Join(f.Root, "records/queries/read.sql")); err != nil {
				t.Fatal(err)
			}
		}},
		{"unknown factory", "not registered", func(t *testing.T, f *fixture.Fixture, c *config.Config) {
			path := filepath.Join(f.Root, "records/records.go")
			data, err := os.ReadFile(path)
			if err != nil {
				t.Fatal(err)
			}
			if err = os.WriteFile(path, []byte(strings.Replace(string(data), "handler=NewWrite", "handler=Unlinked", 1)), 0600); err != nil {
				t.Fatal(err)
			}
		}},
		{"invalid CORS", "CORS", func(t *testing.T, f *fixture.Fixture, c *config.Config) {
			age := int64(-1)
			c.CORS = &spec.CORS{MaxAge: &age}
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			f := fixture.New(t)
			cfg, err := (config.Loader{}).Load(context.Background(), f.Config)
			if err != nil {
				t.Fatal(err)
			}
			tc.change(t, f, cfg)
			exports, err := records.Exports()
			if err != nil {
				t.Fatal(err)
			}
			server, err := New(context.Background(), Options{Config: cfg, Registry: exports})
			if err != nil {
				t.Fatal(err)
			}
			err = server.Reload(context.Background(), 1)
			if err == nil || !strings.Contains(err.Error(), tc.match) {
				t.Errorf("expected %q, got %v", tc.match, err)
			}
			if server.manager.Revision() != 0 {
				t.Error("failed startup published routes")
			}
			if err = server.Shutdown(context.Background()); err != nil {
				t.Fatal(err)
			}
			if err = server.source.connections.SQL.DB.Ping(); err == nil {
				t.Fatal("startup cleanup left database open")
			}
		})
	}
	f := fixture.New(t)
	cfg, err := (config.Loader{}).Load(context.Background(), f.Config)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err = New(ctx, Options{Config: cfg}); !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled startup %v", err)
	}
}

func TestConfiguredMCPUsesManagerPolicy(t *testing.T) {
	f := fixture.New(t)
	policy := &authorization.Policy{Global: &authorization.Authorization{RequiredScopes: []string{"read"}, ProtectedResourceMetadata: &oauthmeta.ProtectedResourceMetadata{Resource: "https://api.example.com"}}}
	f.WriteConfig(t, func(c map[string]any) { c["MCP"] = map[string]any{"Port": 0, "Authorization": policy} })
	cfg, err := (config.Loader{}).Load(context.Background(), f.Config)
	if err != nil {
		t.Fatal(err)
	}
	exports, err := records.Exports()
	if err != nil {
		t.Fatal(err)
	}
	server, err := New(context.Background(), Options{Config: cfg, Registry: exports})
	if err != nil {
		t.Fatal(err)
	}
	defer server.Shutdown(context.Background())
	if err = server.Reload(context.Background(), 1); err != nil {
		t.Fatal(err)
	}
	_, service, err := server.manager.Pin(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if service.Authorization().Global.RequiredScopes[0] != "read" {
		t.Fatal("MCP policy was not published")
	}
	protocol, err := mcpserver.New(mcpserver.Config{Source: server.manager, Transport: mcpserver.TransportConfig{Kind: mcpserver.TransportStreamable}})
	if err != nil {
		t.Fatal(err)
	}
	transport, err := protocol.HTTP()
	if err != nil {
		t.Fatal(err)
	}
	response := httptest.NewRecorder()
	transport.Handler.ServeHTTP(response, httptest.NewRequest("GET", "http://localhost/.well-known/oauth-protected-resource", nil))
	if response.Code != 200 || !strings.Contains(response.Body.String(), "https://api.example.com") {
		t.Fatalf("MCP metadata %d %s", response.Code, response.Body.String())
	}
}
