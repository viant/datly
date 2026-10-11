package standalone

import (
	"context"
	"github.com/stretchr/testify/require"
	"github.com/viant/datly/bootstrap/connector"
	"github.com/viant/datly/exec"
	"github.com/viant/datly/spec"
	"github.com/viant/datly/standalone/config"
	"github.com/viant/datly/standalone/testdata/hiddendiscovery"
	"net/http/httptest"
	"path/filepath"
	"testing"
)

// All database ownership and discovery stays behind the Datly component API;
// the fixture creates no tables and never obtains a raw SQL connection.
func TestStandaloneLinkedHiddenComputedDiscovery(t *testing.T) {
	ctx := context.Background()
	cfg := &config.Config{
		BaseDir:     filepath.Join(t.TempDir(), "no-source"),
		Connector:   "main",
		Connectors:  []connector.Config{{Name: "main", Driver: "sqlite3", DSN: filepath.Join(t.TempDir(), "fixture.db")}},
		GoBootstrap: &config.Packages{Packages: []string{hiddendiscovery.LinkedType.PkgPath()}, LinkedOnly: true},
	}
	server, err := New(ctx, Options{Config: cfg})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, server.Shutdown(ctx)) })
	require.NoError(t, server.Reload(ctx, 1))
	call := func(path string) *httptest.ResponseRecorder {
		response := httptest.NewRecorder()
		server.ServeHTTP(response, httptest.NewRequest("GET", path, nil))
		return response
	}
	good := call("/hidden-discovery")
	require.Equal(t, 200, good.Code, good.Body.String())
	require.JSONEq(t, `{"rows":[{"id":1}]}`, good.Body.String())
	bad := call("/hidden-discovery-unknown")
	require.GreaterOrEqual(t, bad.Code, 400, bad.Body.String())
	_, err = server.InvokeComponent(ctx, exec.ComponentRequest{Target: exec.ComponentTarget{
		Component: spec.Key{Kind: spec.KindComponent, Scope: hiddendiscovery.LinkedType.PkgPath(), Name: "Unknown"},
		Route:     spec.RouteRef{Method: "GET", Path: "/hidden-discovery-unknown"},
	}, Input: &hiddendiscovery.Input{}})
	require.ErrorContains(t, err, "unable discover column unowned type")
}
