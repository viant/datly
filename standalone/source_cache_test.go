package standalone

import (
	"context"
	"encoding/json"
	gateway "github.com/viant/datly/gateway/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/viant/datly/constant"
	"github.com/viant/datly/standalone/config"
	fixture "github.com/viant/datly/standalone/testdata/app"
	"github.com/viant/datly/standalone/testdata/app/cachedrecords"
)

func TestStandaloneNamedNestedCacheSQLite(t *testing.T) {
	for _, mode := range []string{"inline", "legacy", "missing", "disabled", "conflict", "invalidation"} {
		t.Run(mode, func(t *testing.T) {
			ctx := context.Background()
			f := fixture.New(t)
			require.NoError(t, f.DB.ExecStatements(ctx,
				"CREATE TABLE cache_children(id INTEGER,parent_id INTEGER,name TEXT)",
				"CREATE TABLE cache_details(id INTEGER,child_id INTEGER,name TEXT)",
				"INSERT INTO cache_children VALUES(10,1,'child')",
				"INSERT INTO cache_details VALUES(100,10,'detail')"))
			settings := map[string]any{"Name": "shared", "Enabled": mode != "disabled", "Provider": "afs", "Location": filepath.Join(f.Root, "cache/${View.Name}"), "TTL": "1m"}
			f.WriteConfig(t, func(c map[string]any) {
				c["GoBootstrap"] = map[string]any{"Packages": []string{fixture.Module + "/cachedrecords"}, "EagerComponents": true}
				if mode == "invalidation" {
					c["GoBootstrap"] = map[string]any{"Packages": []string{fixture.Module + "/cachedrecords"}}
					c["CacheInvalidation"] = map[string]any{"TimeoutMs": 2000, "Admin": map[string]any{"APIKeyHeader": "X-Admin", "APIKeyValue": "admin"}}
				}
				if mode == "legacy" {
					delete(settings, "Enabled")
					data, err := json.Marshal(map[string]any{"CacheProviders": []any{settings}})
					require.NoError(t, err)
					require.NoError(t, os.WriteFile(filepath.Join(f.Root, "cache.yaml"), data, 0600))
					c["DependencyURL"] = "cache.yaml"
				} else if mode != "missing" {
					c["Caches"] = map[string]any{"shared": settings}
				}
				if mode == "conflict" {
					c["CacheProviders"] = []any{map[string]any{"Name": "shared", "Location": "different", "TTL": "1m"}}
				}
			})
			cfg, err := (config.Loader{}).Load(ctx, f.Config)
			if mode == "conflict" {
				require.ErrorContains(t, err, `conflicting definitions for cache "shared"`)
				return
			}
			require.NoError(t, err)
			// Exercise instance constants together with the per-view location template.
			if mode == "inline" {
				cfg.Const, err = constant.New(map[string]string{"CacheRoot": filepath.Join(f.Root, "cache"), "CacheProvider": "afs"})
				require.NoError(t, err)
				cfg.Caches["shared"].Location = "${CacheRoot}/${View.Name}"
				cfg.Caches["shared"].Provider = "${CacheProvider}"
			}
			s, err := New(ctx, Options{Config: cfg, Registry: cachedrecords.Exports()})
			require.NoError(t, err)
			t.Cleanup(func() { require.NoError(t, s.Shutdown(ctx)) })
			err = s.Reload(ctx, 1)
			if mode == "missing" || mode == "disabled" {
				require.ErrorContains(t, err, `cache "shared" has no enabled configuration`)
				require.Zero(t, s.manager.Revision())
				return
			}
			require.NoError(t, err)
			expectedChild := "child"
			read := func() cachedrecords.Output {
				w := httptest.NewRecorder()
				s.ServeHTTP(w, httptest.NewRequest("GET", "/cached-records", nil))
				require.Equal(t, 200, w.Code, w.Body.String())
				var out cachedrecords.Output
				require.NoError(t, json.Unmarshal(w.Body.Bytes(), &out))
				require.Len(t, out.Rows, 1)
				require.Len(t, out.Rows[0].Children, 1)
				require.Len(t, out.Rows[0].Children[0].Details, 1)
				require.Equal(t, expectedChild, out.Rows[0].Children[0].Name)
				require.Equal(t, "detail", out.Rows[0].Children[0].Details[0].Name)
				return out
			}
			if mode == "invalidation" {
				rejected := httptest.NewRecorder()
				s.ServeHTTP(rejected, httptest.NewRequest("POST", gateway.DefaultCacheInvalidateURI+"/cached-records", strings.NewReader(`{"view":"children","scope":"lazy"}`)))
				require.Equal(t, 403, rejected.Code, "cache control must be preloaded before the first read")
			}
			require.Equal(t, "first", read().Rows[0].Name)
			if mode == "invalidation" {
				require.NoError(t, f.DB.ExecStatements(ctx, "UPDATE cache_children SET name='fresh'"))
				read() // still serves the previous cached child
				request := httptest.NewRequest("POST", gateway.DefaultCacheInvalidateURI+"/cached-records", strings.NewReader(`{"view":"children","scope":"lazy"}`))
				request.Header.Set("X-Admin", "admin")
				response := httptest.NewRecorder()
				s.ServeHTTP(response, request)
				require.Equal(t, 200, response.Code, response.Body.String())
				expectedChild = "fresh"
				read()
			}
			// Only nested views are cached; the parent must still read fresh SQL.
			require.NoError(t, f.DB.ExecStatements(ctx, "DROP TABLE cache_children", "DROP TABLE cache_details", "UPDATE records SET name='updated'"))
			require.Equal(t, "updated", read().Rows[0].Name)
			for _, name := range []string{"children", "details"} {
				entries, err := os.ReadDir(filepath.Join(f.Root, "cache", name))
				require.NoError(t, err)
				require.NotEmpty(t, entries)
			}
		})
	}
}
