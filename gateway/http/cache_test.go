package http

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/viant/datly/bootstrap"
	druntime "github.com/viant/datly/runtime"
	"github.com/viant/datly/spec"
	dsql "github.com/viant/datly/sql"
	"github.com/viant/sqlx/io/read/cache/aerospike"
	stdhttp "net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	dexec "github.com/viant/datly/exec"
)

func cacheConfig() Config {
	return Config{APIPrefix: "/api", Meta: Meta{CacheWarmURI: " ", CacheInvalidateURI: "/admin/cache"}, CacheInvalidation: &CacheInvalidationConfig{Timeout: time.Second, Authorize: func(_ context.Context, r *stdhttp.Request, target dexec.ComponentTarget) error {
		if r.Header.Get("X-Admin") != "admin" || target.Route.Path != "/api/records" {
			return errors.New("denied")
		}
		return nil
	}}}
}
func TestCacheInvalidationLazyOnlyHTTP(t *testing.T) {
	f := newConfigFixture(t)
	f.component.Settings.Cache.Warmup = nil
	rt := f.runtime(t)
	recorder := rt.Observability().Recorder
	handler, err := cacheConfig().NewHandler(rt, nil, "test")
	require.NoError(t, err)
	get := func() string {
		w := httptest.NewRecorder()
		handler.ServeHTTP(w, httptest.NewRequest("GET", "/api/records?tenant=1", nil))
		require.Equal(t, 200, w.Code, w.Body.String())
		return w.Body.String()
	}
	before := get()
	require.Contains(t, before, "11")
	require.NoError(t, f.db.ExecStatements(context.Background(), "UPDATE records SET id=99 WHERE id=11"))
	require.Equal(t, before, get())
	request := func(body, key string) *httptest.ResponseRecorder {
		r := httptest.NewRequest("POST", "/admin/cache/records", strings.NewReader(body))
		r.Header.Set("X-Admin", key)
		w := httptest.NewRecorder()
		handler.ServeHTTP(w, r)
		return w
	}
	denied := request(`{"scope":"all"}`, "")
	require.Equal(t, 403, denied.Code)
	require.Equal(t, before, get())
	invalid := request(`{"scope":"wrong"}`, "admin")
	require.Equal(t, 400, invalid.Code)
	missing := request(`{"view":"missing"}`, "admin")
	require.Equal(t, 404, missing.Code)
	success := request(`{"scope":"lazy"}`, "admin")
	require.Equal(t, 200, success.Code, success.Body.String())
	var body map[string]interface{}
	require.NoError(t, json.Unmarshal(success.Body.Bytes(), &body))
	require.Equal(t, "ok", body["status"])
	after := get()
	require.Contains(t, after, "99")
	require.NotEqual(t, before, after)
	scope := f.component.Key.String() + "/records"
	require.Equal(t, int64(2), recorder.Cumulative(scope, "cache:created"))
	require.Equal(t, int64(2), recorder.Cumulative(scope, "cache:lazy_created"))
}
func TestCacheInvalidationRequiresAdminPolicy(t *testing.T) {
	f := newConfigFixture(t)
	f.component.Settings.Cache.Warmup = nil
	config := cacheConfig()
	config.CacheInvalidation.Authorize = nil
	_, err := config.NewHandler(f.runtime(t), nil, "test")
	require.ErrorContains(t, err, "administrator")
}

func TestCacheInvalidationChildAndProviderLocation(t *testing.T) {
	type parent struct {
		ID       int              `sqlx:"id" json:"id"`
		Children []warmupChildRow `sqlx:"-" json:"children"`
	}
	type output struct {
		Rows []parent `json:"rows"`
	}
	ctx := context.Background()
	f := newConfigFixture(t)
	f.component.Settings.Cache = nil
	require.NoError(t, f.db.ExecStatements(ctx, "CREATE TABLE child_a(id INTEGER,parent_id INTEGER,name TEXT)", "INSERT INTO child_a VALUES(111,11,'before')"))
	child := &spec.View{Name: "children", Namespace: "c", Source: &spec.ViewSource{Table: "child_a", SQL: "SELECT c.id,c.parent_id,c.name FROM child_a c WHERE $COLUMN_IN", Bindings: &spec.ViewBindings{CacheName: "shared"}}}
	f.component.RootView.Relations = []*spec.Relation{{Name: "children", Holder: "Children", Cardinality: spec.CardinalityMany, On: []*spec.RelationLink{{ParentColumn: "id", ChildColumn: "parent_id"}}, View: child}}
	artifact, err := bootstrap.BuildArtifact(bootstrap.ArtifactInput{Component: f.component, InputType: reflect.TypeFor[configInput](), OutputType: reflect.TypeFor[output](), DirectViewField: "Rows"})
	require.NoError(t, err)
	root := t.TempDir()
	reader, err := artifact.ReaderCompilation().NewExecution(bootstrap.ReaderRuntimeConfig{SQL: &dsql.SQLComponent{DB: f.db.DB}, CacheSettings: map[string]*spec.CacheSettings{"shared": {Enabled: true, Location: filepath.Join(root, "${View.Name}", "${View.Alias}", "${View.Table}"), TTL: "1m"}}})
	require.NoError(t, err)
	rt, err := druntime.NewRuntime([]*druntime.RegisteredComponent{{Component: artifact.Component, Input: artifact.Input, OutputType: reflect.TypeFor[output](), Reader: reader}})
	require.NoError(t, err)
	handler, err := cacheConfig().NewHandler(rt, nil, "test")
	require.NoError(t, err)
	read := func() string {
		w := httptest.NewRecorder()
		handler.ServeHTTP(w, httptest.NewRequest("GET", "/api/records?tenant=1", nil))
		require.Equal(t, 200, w.Code, w.Body.String())
		return w.Body.String()
	}
	before := read()
	require.Contains(t, before, "before")
	_, err = os.Stat(filepath.Join(root, "children", "c", "child_a"))
	require.NoError(t, err)
	require.NoError(t, f.db.ExecStatements(ctx, "UPDATE child_a SET name='after'"))
	require.Equal(t, before, read())
	req := httptest.NewRequest("POST", "/admin/cache/records", strings.NewReader(`{"view":"children","scope":"lazy"}`))
	req.Header.Set("X-Admin", "admin")
	w := httptest.NewRecorder()
	handler.ServeHTTP(w, req)
	require.Equal(t, 200, w.Code, w.Body.String())
	require.Contains(t, read(), "after")
}
func TestCacheCreationTimestampInHTTPMetrics(t *testing.T) {
	f := newConfigFixture(t)
	f.component.Settings.Cache.Warmup = nil
	config := cacheConfig()
	config.Metrics = &MetricsConfig{}
	handler, err := config.NewHandler(f.runtime(t), nil, "test")
	require.NoError(t, err)
	var created string
	for i := 0; i < 2; i++ {
		req := httptest.NewRequest("GET", "/api/records?tenant=1", nil)
		req.Header.Set(datlyRequestMetricsHeader, "true")
		w := httptest.NewRecorder()
		handler.ServeHTTP(w, req)
		require.Equal(t, 200, w.Code)
		found := false
		for name, values := range w.Header() {
			if !strings.HasPrefix(name, "Datly-Metrics-") {
				continue
			}
			for _, value := range values {
				var metric struct {
					Executions []struct {
						CacheStats struct {
							CreatedTime string `json:"createdTime"`
							ExpiryTime  string `json:"expiryTime"`
						} `json:"cacheStats"`
					} `json:"executions"`
				}
				if json.Unmarshal([]byte(value), &metric) != nil {
					continue
				}
				for _, execution := range metric.Executions {
					if execution.CacheStats.CreatedTime != "" {
						found = true
						require.NotEmpty(t, execution.CacheStats.ExpiryTime)
						if i == 0 {
							created = execution.CacheStats.CreatedTime
						} else {
							require.Equal(t, created, execution.CacheStats.CreatedTime)
						}
					}
				}
			}
		}
		require.True(t, found, "creation timestamp must reach diagnostic headers")
	}
}

func TestCacheInvalidationAerospikeHTTPAcrossRuntimes(t *testing.T) {
	uri := os.Getenv("DATLY_TEST_AEROSPIKE")
	if uri == "" {
		t.Skip("dedicated Aerospike test instance is required")
	}
	f := newConfigFixture(t)
	f.component.Settings.Cache.Warmup = nil
	f.component.Key.Name = "Records" + fmt.Sprint(time.Now().UnixNano())
	f.component.Settings.Cache.Provider = uri
	f.component.Settings.Cache.Location = "datly_validation"
	f.pool = &aerospike.Pool{}
	t.Cleanup(func() { require.NoError(t, f.pool.Close()) })
	first, err := cacheConfig().NewHandler(f.runtime(t), nil, "test")
	require.NoError(t, err)
	second, err := cacheConfig().NewHandler(f.runtime(t), nil, "test")
	require.NoError(t, err)
	get := func(h *Handler) string {
		w := httptest.NewRecorder()
		h.ServeHTTP(w, httptest.NewRequest("GET", "/api/records?tenant=1", nil))
		require.Equal(t, 200, w.Code, w.Body.String())
		return w.Body.String()
	}
	before := get(first)
	require.Equal(t, before, get(second))
	require.NoError(t, f.db.ExecStatements(context.Background(), "UPDATE records SET id=99 WHERE id=11"))
	require.Equal(t, before, get(second))
	req := httptest.NewRequest("POST", "/admin/cache/records", strings.NewReader(`{"scope":"all"}`))
	req.Header.Set("X-Admin", "admin")
	w := httptest.NewRecorder()
	first.ServeHTTP(w, req)
	require.Equal(t, 200, w.Code, w.Body.String())
	require.Contains(t, get(second), "99")
}
func TestCacheInvalidationWarmupHTTPAndMetrics(t *testing.T) {
	ctx := context.Background()
	f := newConfigFixture(t)
	rt := f.runtime(t)
	config := warmupConfig()
	config.Meta.CacheInvalidateURI = "/admin/cache"
	config.CacheInvalidation = cacheConfig().CacheInvalidation
	h, err := config.NewHandler(rt, nil, "test")
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, h.Shutdown(ctx)) })
	post := func(path, body string) {
		req := httptest.NewRequest("POST", path, strings.NewReader(body))
		req.Header.Set("X-Admin", "admin")
		w := httptest.NewRecorder()
		h.ServeHTTP(w, req)
		require.Equal(t, 200, w.Code, w.Body.String())
	}
	get := func() string {
		w := httptest.NewRecorder()
		h.ServeHTTP(w, httptest.NewRequest("GET", "/api/records?tenant=1", nil))
		require.Equal(t, 200, w.Code, w.Body.String())
		return w.Body.String()
	}
	post("/admin/warm/records", "")
	before := get()
	require.Contains(t, before, "11")
	require.NoError(t, f.db.ExecStatements(ctx, "UPDATE records SET id=99 WHERE id=11"))
	require.Equal(t, before, get())
	post("/admin/cache/records", `{"scope":"warmup"}`)
	require.Contains(t, get(), "99")
	post("/admin/warm/records", "")
	require.NoError(t, f.db.ExecStatements(ctx, "DROP TABLE records"))
	require.Contains(t, get(), "99")
	scope := f.component.Key.String() + "/records"
	require.Equal(t, int64(4), rt.Observability().Recorder.Cumulative(scope, "cache:warmup_created"))
	require.Equal(t, int64(1), rt.Observability().Recorder.Cumulative(scope, "cache:lazy_created"))
}
