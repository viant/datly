package standalone

import (
	"context"
	"fmt"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/viant/datly/internal/drainowner"
	"github.com/viant/datly/standalone/config"
	fixture "github.com/viant/datly/standalone/testdata/app"
	"github.com/viant/datly/standalone/testdata/scopedsourced"
)

func TestStandaloneScopedFactorySQLViewBinding(t *testing.T) {
	for _, caller := range []bool{false, true} {
		for _, mode := range []string{"success", "missing-default", "missing-lazy", "default-SQL", "lazy-SQL", "validation", "handler", "late", "caller", "denied", "skip-lazy", "grouped-context"} {
			if !caller && mode == "caller" {
				continue
			}
			t.Run(fmt.Sprintf("caller=%v/%s", caller, mode), func(t *testing.T) {
				f := fixture.New(t)
				require.NoError(t, f.DB.ExecStatements(t.Context(), "CREATE TABLE scoped_audit(id INTEGER PRIMARY KEY,name TEXT NOT NULL)", "CREATE TABLE scoped_current(id INTEGER PRIMARY KEY,name TEXT)", "INSERT INTO scoped_current VALUES(11,'current')", "CREATE TABLE scoped_later(id INTEGER PRIMARY KEY,name TEXT)", "INSERT INTO scoped_later VALUES(22,'later')"))
				if mode == "default-SQL" {
					require.NoError(t, f.DB.ExecStatements(t.Context(), "DROP TABLE scoped_current"))
				}
				if mode == "lazy-SQL" || mode == "denied" || mode == "skip-lazy" {
					require.NoError(t, f.DB.ExecStatements(t.Context(), "DROP TABLE scoped_later"))
				}
				f.WriteConfig(t, func(c map[string]any) {
					c["GoBootstrap"] = map[string]any{"Packages": []string{"github.com/viant/datly/standalone/testdata/scopedsourced"}, "LinkedOnly": true}
				})
				cfg, err := (config.Loader{}).Load(t.Context(), f.Config)
				require.NoError(t, err)
				cfg.BaseDir = filepath.Join(t.TempDir(), "no-sources")
				_ = scopedsourced.LinkedType
				server, err := New(t.Context(), Options{Config: cfg})
				require.NoError(t, err)
				t.Cleanup(func() { require.NoError(t, server.Shutdown(context.Background())) })
				require.NoError(t, server.Reload(t.Context(), 1))
				path := "/scoped-source"
				if caller {
					path = "/scoped-parent"
				}
				req := httptest.NewRequest("POST", path+"?mode="+mode+"&selection=11", strings.NewReader(`{}`))
				req.Header.Set("Content-Type", "application/json")
				if mode != "missing-default" {
					credential := "allowed"
					if mode == "denied" {
						credential = "deny"
					}
					req.Header.Set("Authorization", credential)
				}
				if mode != "missing-lazy" {
					req.Header.Set("X-Lazy", "later")
				}
				if mode == "grouped-context" {
					inv := drainowner.NewInvocation()
					require.NoError(t, drainowner.EnrollActivities(inv))
					g, e := drainowner.OpenBindingGroup(inv, []drainowner.BindingGroupMember{{Path: "custom", Target: "POST:" + path}})
					require.NoError(t, e)
					ctx, e := g.Enter(req.Context(), "custom")
					require.NoError(t, e)
					req = req.WithContext(ctx)
					defer func() { _ = g.Close() }()
				}
				response := httptest.NewRecorder()
				server.ServeHTTP(response, req)
				wantCount := 0
				if mode == "skip-lazy" {
					require.Equal(t, 200, response.Code, response.Body.String())
					require.Contains(t, response.Body.String(), `"stage":"default,handler"`)
					if caller {
						wantCount = 1
					}
				} else if mode == "success" {
					require.Equal(t, 200, response.Code, response.Body.String())
					require.Contains(t, response.Body.String(), `"stage":"default,handler,validated,queued,lazy"`)
					wantCount = 1
					if caller {
						wantCount = 2
					}
				} else {
					require.GreaterOrEqual(t, response.Code, 400, response.Body.String())
				}
				if mode == "denied" {
					require.Equal(t, 403, response.Code, response.Body.String())
				}
				if mode == "missing-default" {
					require.Equal(t, 401, response.Code, response.Body.String())
				}
				if mode == "validation" {
					require.Equal(t, 422, response.Code, response.Body.String())
				}
				if mode == "handler" || mode == "late" {
					require.Equal(t, 500, response.Code, response.Body.String())
				}
				var count int
				require.NoError(t, f.DB.DB.QueryRow("SELECT COUNT(*) FROM scoped_audit").Scan(&count))
				require.Equal(t, wantCount, count, response.Body.String())
			})
		}
	}

}

func TestStandaloneScopedSQLViewSourceLessCaller(t *testing.T) {
	for _, mode := range []string{"success", "caller", "late"} {
		t.Run(mode, func(t *testing.T) {
			f := fixture.New(t)
			require.NoError(t, f.DB.ExecStatements(t.Context(), "CREATE TABLE scoped_audit(id INTEGER PRIMARY KEY,name TEXT NOT NULL)", "CREATE TABLE scoped_current(id INTEGER PRIMARY KEY,name TEXT)", "INSERT INTO scoped_current VALUES(11,'current')", "CREATE TABLE scoped_later(id INTEGER PRIMARY KEY,name TEXT)", "INSERT INTO scoped_later VALUES(22,'later')"))
			f.WriteConfig(t, func(c map[string]any) {
				c["Connector"] = ""
				c["GoBootstrap"] = map[string]any{"Packages": []string{"github.com/viant/datly/standalone/testdata/scopedsourced"}, "LinkedOnly": true}
			})
			cfg, err := (config.Loader{}).Load(t.Context(), f.Config)
			require.NoError(t, err)
			cfg.BaseDir = filepath.Join(t.TempDir(), "no-source")
			server, err := New(t.Context(), Options{Config: cfg})
			require.NoError(t, err)
			t.Cleanup(func() { require.NoError(t, server.Shutdown(context.Background())) })
			require.NoError(t, server.Reload(t.Context(), 1))
			req := httptest.NewRequest("POST", "/scoped-nosource?selection=11&mode="+mode, strings.NewReader(`{}`))
			req.Header.Set("Content-Type", "application/json")
			req.Header.Set("Authorization", "allowed")
			req.Header.Set("X-Lazy", "later")
			response := httptest.NewRecorder()
			server.ServeHTTP(response, req)
			want := 0
			if mode == "success" {
				require.Equal(t, 200, response.Code, response.Body.String())
				want = 1
			} else {
				require.Equal(t, 500, response.Code, response.Body.String())
			}
			var count int
			require.NoError(t, f.DB.DB.QueryRow("SELECT COUNT(*) FROM scoped_audit").Scan(&count))
			require.Equal(t, want, count, response.Body.String())
		})
	}
}
