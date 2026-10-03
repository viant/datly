package standalone

import (
	"context"
	"net/http/httptest"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
	gateway "github.com/viant/datly/gateway/http"
	"github.com/viant/datly/spec"
	"github.com/viant/datly/standalone/config"
	fixture "github.com/viant/datly/standalone/testdata/app"
	"github.com/viant/datly/standalone/testdata/childwarmup"
)

func TestStandaloneLinkedOnlyColdChildWarmup(t *testing.T) {
	ctx := context.Background()
	f := fixture.New(t)
	require.NoError(t, f.DB.ExecStatements(ctx,
		"CREATE TABLE warm_timeline(parent_id INTEGER,value INTEGER)",
		"INSERT INTO warm_timeline VALUES(1,11)",
		"CREATE TABLE warm_summary(parent_id INTEGER,value INTEGER)",
		"INSERT INTO warm_summary VALUES(1,77)",
	))
	cfg, err := (config.Loader{}).Load(ctx, f.Config)
	require.NoError(t, err)
	cfg.GoBootstrap = &config.Packages{Packages: []string{childwarmup.LinkedType.PkgPath()}, LinkedOnly: true}
	cfg.BaseDir = filepath.Join(t.TempDir(), "no-source")
	cfg.Caches = map[string]*spec.CacheSettings{
		"shared":         {Enabled: true, Provider: "afs", TTL: "1h", Location: filepath.Join(t.TempDir(), "${View.Name}")},
		"timelineWarmup": {Warmup: &spec.CacheWarmupSettings{Connector: "main", IndexColumn: "parent_id"}},
		"summaryWarmup":  {Warmup: &spec.CacheWarmupSettings{Connector: "main", IndexColumn: "parent_id"}},
	}
	cfg.Warmup = &config.Warmup{TimeoutMs: 2000, Admin: &gateway.DocumentAccess{APIKeyHeader: "X-Admin", APIKeyValue: "admin-key"}}
	diagnostics := &synchronizedBuffer{}
	server, err := New(ctx, Options{Config: cfg, Diagnostics: diagnostics})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, server.Shutdown(ctx)) })
	require.NoError(t, server.Reload(ctx, 1))
	require.Contains(t, diagnostics.text(), `"preload":1`)
	require.NotContains(t, diagnostics.text(), ":Unrelated")
	call := func(method, path string, read, admin bool) *httptest.ResponseRecorder {
		req := httptest.NewRequest(method, path, nil)
		if read {
			req.Header.Set("X-Read", "read-key")
		}
		if admin {
			req.Header.Set("X-Admin", "admin-key")
		}
		res := httptest.NewRecorder()
		server.ServeHTTP(res, req)
		return res
	}
	// All requests before warmup are warmup POSTs, never reads or MCP discovery.
	path := gateway.DefaultCacheWarmURI + "/child-warmup"
	for _, credentials := range [][2]bool{{false, true}, {true, false}, {false, false}} {
		res := call("POST", path, credentials[0], credentials[1])
		require.Equal(t, 403, res.Code, res.Body.String())
	}
	warm := call("POST", path, true, true)
	require.Equal(t, 200, warm.Code, warm.Body.String())
	require.NoError(t, f.DB.ExecStatements(ctx, "DROP TABLE warm_timeline", "DROP TABLE warm_summary"))
	read := call("GET", "/child-warmup", true, false)
	require.Equal(t, 200, read.Code, read.Body.String())
	require.JSONEq(t, `{"rows":[{"id":1,"name":"first","timeline":[{"parentId":1,"value":11}],"summary":[{"parentId":1,"value":77}]}]}`, read.Body.String())
	require.NotContains(t, diagnostics.text(), ":Unrelated", "unrelated components must stay lazy")
}

func TestStandaloneLinkedOnlyChildWarmupRemainsLazyWhenDisabled(t *testing.T) {
	ctx := context.Background()
	f := fixture.New(t)
	cfg, err := (config.Loader{}).Load(ctx, f.Config)
	require.NoError(t, err)
	cfg.GoBootstrap = &config.Packages{Packages: []string{childwarmup.LinkedType.PkgPath()}, LinkedOnly: true}
	cfg.BaseDir = filepath.Join(t.TempDir(), "no-source")
	diagnostics := &synchronizedBuffer{}
	server, err := New(ctx, Options{Config: cfg, Diagnostics: diagnostics})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, server.Shutdown(ctx)) })
	require.NoError(t, server.Reload(ctx, 1))
	require.Contains(t, diagnostics.text(), `"preload":0`)
	require.NotContains(t, diagnostics.text(), "datly bootstrap linked materialize")
}
