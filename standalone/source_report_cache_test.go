package standalone

import (
	"context"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	gateway "github.com/viant/datly/gateway/http"
	"github.com/viant/datly/spec"
	"github.com/viant/datly/standalone/config"
	fixture "github.com/viant/datly/standalone/testdata/app"
	dtag "github.com/viant/datly/tag"
)

func TestStandaloneDiscoveredCubeNativeCacheSQLite(t *testing.T) {
	s, f, _ := newStandaloneReport(t, func(f *fixture.Fixture, cfg *config.Config) {
		// Author cache metadata in the discovered package, as a configured custom
		// build does. The linked row type and SQLX tags remain unchanged.
		settings, err := (dtag.Settings{Cache: &spec.CacheSettings{Enabled: true, TTL: "1m", Location: filepath.Join(f.Root, "cube-cache"), Warmup: &spec.CacheWarmupSettings{
			Cases: []*spec.CacheWarmupCase{{Set: []*spec.CacheWarmupParam{{Name: "Tenant", Values: []string{"acme"}}, {Name: "Channel", Values: []string{"web"}}}}},
		}}}).StructTag()
		require.NoError(t, err)
		path := filepath.Join(f.Root, "spend/spend.go")
		code, err := os.ReadFile(path)
		require.NoError(t, err)
		code = []byte(strings.Replace(string(code), "`"+`component:"Spend,`, "`"+settings+` component:"Spend,`, 1))
		require.NoError(t, os.WriteFile(path, code, 0600))
		cfg.Warmup = &config.Warmup{TimeoutMs: 2000, Admin: &gateway.DocumentAccess{APIKeyHeader: "X-Admin", APIKeyValue: "admin"}}
	})
	call := func(method, path, body string, admin bool) *httptest.ResponseRecorder {
		r := httptest.NewRequest(method, path, strings.NewReader(body))
		r.Header.Set("Content-Type", "application/json")
		r.Header.Set("X-Tenant", "acme")
		r.Header.Set("Cookie", "channel=web")
		if admin {
			r.Header.Set("X-Admin", "admin")
		}
		w := httptest.NewRecorder()
		s.ServeHTTP(w, r)
		return w
	}
	warm := gateway.DefaultCacheWarmURI + "/spend"
	w := call("POST", warm, "", false)
	require.Equal(t, 403, w.Code, w.Body.String())
	w = call("POST", warm, "", true)
	require.Equal(t, 200, w.Code, w.Body.String())
	// Delete the table immediately after full warmup: the first narrowed cube
	// request must reuse the warmed projection, not create a narrower cache entry.
	require.NoError(t, f.DB.ExecStatements(context.Background(), "DROP TABLE report_spend"))
	// Both full and narrower measure projections keep every grouping dimension.
	for _, body := range []string{spendCube, strings.Replace(spendCube, `"totalSpend":true`, `"totalSpend":true,"orderCount":true`, 1)} {
		w = call("POST", "/spend/cube", body, false)
		require.Equal(t, 200, w.Code, w.Body.String())
		assertStandaloneReport(t, w.Body.Bytes(), "cube", 150)
	}
	// Replay after deleting the source table proves this uses the actual native
	// reader cache wired by standalone, rather than merely rerunning SQLite.
	w = call("POST", "/spend/cube", spendCube, false)
	require.Equal(t, 200, w.Code, w.Body.String())
	assertStandaloneReport(t, w.Body.Bytes(), "cube", 150)
	w = call("GET", "/spend", "", false)
	require.Equal(t, 200, w.Code, w.Body.String())
	assertStandaloneReport(t, w.Body.Bytes(), "cube", 150)
	// A different grouping cannot be answered by hiding a cached dimension.
	w = call("POST", "/spend/cube", strings.Replace(spendCube, `,"region":true`, "", 1), false)
	require.NotEqual(t, 200, w.Code, w.Body.String())
}
