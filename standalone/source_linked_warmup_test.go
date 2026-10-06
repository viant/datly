package standalone

import (
	"context"
	"encoding/json"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"testing"
	"time"

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
	cfg.Observation = &config.Observation{LogSummaries: true}
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
	require.NotContains(t, diagnostics.text(), "datly cache warmup started", "denied requests must not log a start")
	before := len(diagnostics.text())
	warm := call("POST", path, true, true)
	require.Equal(t, 200, warm.Code, warm.Body.String())
	var traceID string
	views := 0
	started := 0
	completed := 0
	var target string
	for _, line := range strings.Split(strings.TrimSpace(diagnostics.text()[before:]), "\n") {
		var record map[string]any
		require.NoError(t, json.Unmarshal([]byte(line), &record))
		switch record["msg"] {
		case "datly view read", "datly cache read", "datly cache warmup started", "datly cache warmup completed":
			id, _ := record["reqTraceId"].(string)
			require.NotEmpty(t, id)
			require.NotEqual(t, "unknown", id)
			if traceID == "" {
				traceID = id
			}
			require.Equal(t, traceID, id)
			if record["msg"] == "datly cache warmup started" {
				require.Zero(t, views, "start must precede preparation/read summaries")
				require.Zero(t, completed)
				require.Equal(t, "INFO", record["level"])
				target, _ = record["target"].(string)
				require.NotEmpty(t, target)
				started++
			}
			if record["msg"] == "datly view read" {
				views++
			}
			if record["msg"] == "datly cache warmup completed" {
				require.Equal(t, 1, started)
				require.Equal(t, target, record["target"])
				elapsed, err := time.ParseDuration(record["elapsed"].(string))
				require.NoError(t, err)
				require.Positive(t, elapsed)
				completed++
			}
		}
	}
	require.GreaterOrEqual(t, views, 2, "both child caches must have correlated population summaries")
	require.Equal(t, 1, started)
	require.Equal(t, 1, completed)
	for _, secret := range []string{"read-key", "admin-key", "SELECT "} {
		require.NotContains(t, diagnostics.text()[before:], secret)
	}
	require.NoError(t, f.DB.ExecStatements(ctx, "DROP TABLE warm_timeline", "DROP TABLE warm_summary"))
	before = len(diagnostics.text())
	read := call("GET", "/child-warmup", true, false)
	require.Equal(t, 200, read.Code, read.Body.String())
	require.NotContains(t, diagnostics.text()[before:], traceID, "ordinary reads must not inherit the warmup context")
	require.NotContains(t, diagnostics.text()[before:], `"reqTraceId":"unknown"`)
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
