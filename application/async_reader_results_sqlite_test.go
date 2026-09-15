package application_test

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http/httptest"
	"net/url"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/viant/datly/application"
	"github.com/viant/datly/bootstrap"
	"github.com/viant/datly/exec"
	gateway "github.com/viant/datly/gateway/http"
	authfixture "github.com/viant/datly/internal/testharness/auth"
	"github.com/viant/datly/observability"
	rauth "github.com/viant/datly/runtime/auth"
	rhandler "github.com/viant/datly/runtime/handler"
	"github.com/viant/datly/runtime/jobs"
	"github.com/viant/datly/runtime/registry"
	"github.com/viant/datly/spec"
	dsql "github.com/viant/datly/sql"
	"github.com/viant/datly/typecatalog"
	"github.com/viant/scy/auth/jwt"
	"github.com/viant/sqlx"
	"github.com/viant/sqlx/io/read/cache"
	afscache "github.com/viant/sqlx/io/read/cache/afs"
	xasync "github.com/viant/xdatly/async"
	xhandler "github.com/viant/xdatly/handler"
	xresponse "github.com/viant/xdatly/response"
)

type readerFilter struct{ Rows []*readerFilterRow }
type readerFilterRow struct{ Permit int }
type readerFilterFlag struct{ Permit int }

type readerResultInput struct {
	Permit     *readerFilterFlag
	Current    *readerFilter
	ID, Tenant int
	Key        string
	Sync       bool
	JWT        *jwt.Claims
}
type readerResultInspect struct {
	JobID      string
	ID, Tenant int
	Sync       bool
	JWT        *jwt.Claims
}
type readerResultChild struct {
	ParentID int `sqlx:"parent_id"`
	Name     string
}

type readerResultRow struct {
	Children   []*readerResultChild `sqlx:"-" json:"children,omitempty"`
	ID, Tenant int
	Name       string
}
type readerResultOutput struct {
	Rows   []*readerResultRow `json:"rows,omitempty"`
	Job    *xasync.Job        `parameter:"kind=async,in=job" json:"job,omitempty"`
	Code   string             `parameter:"kind=async,in=jobinfo.code" json:"code"`
	Status string             `parameter:"kind=output,in=status" json:"status"`
}
type readerResultsFixture struct {
	asyncAppFixture
	cacheDir      string
	relations     bool
	withoutLookup bool
	dynamic       bool
	jwt           *authfixture.JWT
	token         string
	revoked       atomic.Bool
	allowedTenant atomic.Int32
	reads         atomic.Int32
	gate          func(context.Context) error
	sql           string
}

func (f *readerResultsFixture) init(t *testing.T) {
	f.asyncAppFixture.init(t, true)
	f.cacheDir = t.TempDir()
	f.jwt = authfixture.NewJWT(t)
	f.token = f.jwt.Sign(t, time.Now().Add(time.Hour))
	f.allowedTenant.Store(1)
	require.NoError(t, f.db.ExecStatements(context.Background(), "CREATE TABLE result_records(id INTEGER,tenant INTEGER,name TEXT)", "INSERT INTO result_records VALUES(7,1,'cached tenant one'),(7,2,'cached tenant two')"))
	f.sql = "SELECT id,tenant,name FROM result_records WHERE id=:ID AND tenant=:Tenant"
	f.config.Authorize = func(_ context.Context, access jobs.Access) error {
		if f.revoked.Load() {
			return &xresponse.Error{Code: 403, Payload: xresponse.Status{Status: "error", Message: "revoked"}}
		}
		var claim *jwt.Claims
		var tenant int
		switch input := access.Input.(type) {
		case *readerResultInput:
			claim, tenant = input.JWT, input.Tenant
		case *readerResultInspect:
			claim, tenant = input.JWT, input.Tenant
		}
		if input, ok := access.Input.(*readerResultInput); ok && input.Current != nil {
			return fmt.Errorf("dynamic Current was restored during preflight")
		}
		if access.Input != nil && (claim == nil || claim.Subject != "approved" || tenant != int(f.allowedTenant.Load())) {
			return &xresponse.Error{Code: 403, Payload: xresponse.Status{Status: "error", Message: "tenant denied"}}
		}
		return nil
	}
}
func (f *readerResultsFixture) compile(ctx context.Context, _ *typecatalog.Catalog) (*application.Build, error) {
	required := true
	jwtParam := func() *spec.Parameter {
		return &spec.Parameter{Name: "JWT", TypeExpr: "string", Source: spec.BindSource{Kind: "header", Name: "Authorization"}, Codec: &spec.Codec{Body: rauth.JwtClaim}, Required: &required, ErrorStatusCode: 401, ErrorMessage: "invalid authentication"}
	}
	params := []*spec.Parameter{{Name: "ID", Source: spec.BindSource{Kind: "query", Name: "id"}, Required: &required}, {Name: "Tenant", Source: spec.BindSource{Kind: "query", Name: "tenant"}, Required: &required}, {Name: "Key", Source: spec.BindSource{Kind: "query", Name: "key"}}, {Name: "Sync", Source: spec.BindSource{Kind: "query", Name: "sync"}}, jwtParam(), {Name: "Rows", Source: spec.BindSource{Kind: "output", Name: "view"}}}
	component := &spec.Component{Key: spec.Key{Kind: spec.KindComponent, Name: "Results", Scope: "example.com/results"}, Name: "Results", Routes: []*spec.Route{{Method: "GET", Path: "/read-results", APIKeyHeader: "X-Key", APIKeyValue: "reader-key"}}, Parameters: params, Settings: &spec.Settings{Cache: &spec.CacheSettings{Enabled: true, Name: "results", Location: f.cacheDir, TTL: "1h"}}, RootView: &spec.View{Name: "Results", Source: &spec.ViewSource{SQL: f.sql}}}
	if f.dynamic {
		component.Parameters = append(component.Parameters, &spec.Parameter{Name: "Current", Source: spec.BindSource{Kind: "component", Name: "GET:/result-filter"}})
		component.Parameters = append(component.Parameters, &spec.Parameter{Name: "Permit", Source: spec.BindSource{Kind: "param", Name: "Current"}, Codec: &spec.Codec{Body: "structql", Args: []string{"SELECT Permit FROM `/Rows` LIMIT 1"}}})
		component.RootView.Source.SQL += " AND $Permit.Permit = 1"
	}
	if f.relations {
		component.RootView.Relations = []*spec.Relation{{Name: "children", Holder: "Children", Cardinality: spec.CardinalityMany, On: []*spec.RelationLink{{ParentColumn: "id", ChildColumn: "parent_id"}}, View: &spec.View{Name: "children", Source: &spec.ViewSource{SQL: "SELECT parent_id,name FROM result_children WHERE $COLUMN_IN"}}}}
	}
	artifact, err := bootstrap.BuildArtifact(bootstrap.ArtifactInput{Component: component, InputType: reflect.TypeFor[readerResultInput](), OutputType: reflect.TypeFor[readerResultOutput](), DirectViewField: "Rows", CodecFactory: f.jwt.Factory})
	if err != nil {
		return nil, err
	}
	readerConfig := bootstrap.ReaderRuntimeConfig{SQL: &dsql.SQLComponent{DB: f.db.DB}}
	if f.relations || f.withoutLookup {
		native, err := afscache.NewCache(f.cacheDir, time.Hour, "graph", nil)
		if err != nil {
			return nil, err
		}
		readerConfig.ReadCaches = map[string]cache.Cache{"Results": native}
		if f.relations {
			readerConfig.ReadCaches["children"] = native
		}
		if f.withoutLookup {
			// Deliberately expose only the original cache.Cache contract.
			readerConfig.ReadCaches["Results"] = struct{ cache.Cache }{native}
		}
	}
	reader, err := artifact.ReaderCompilation().NewExecution(readerConfig)
	if err != nil {
		return nil, err
	}
	reader = &countedResultReader{Reader: reader, calls: &f.reads, gate: f.gate}
	components := []*registry.RegisteredComponent{{Component: artifact.Component, Input: artifact.Input, Output: artifact.Output, OutputType: reflect.TypeFor[readerResultOutput](), Reader: reader}}
	routes := []gateway.AsyncRoute{{Route: spec.RouteRef{Method: "GET", Path: "/read-results"}, MatchKey: "Key", SyncFlag: "Sync"}}
	if f.dynamic {
		filter := &spec.Component{Key: spec.Key{Kind: spec.KindComponent, Name: "Filter", Scope: "example.com/results"}, Name: "Filter", Routes: []*spec.Route{{Method: "GET", Path: "/result-filter"}}, Parameters: []*spec.Parameter{{Name: "Tenant", Source: spec.BindSource{Kind: "query", Name: "tenant"}}, {Name: "Rows", Source: spec.BindSource{Kind: "output", Name: "view"}}}, RootView: &spec.View{Name: "Filter", Source: &spec.ViewSource{SQL: "SELECT allowed AS permit FROM result_filter WHERE tenant=:Tenant"}}}
		compiled, err := bootstrap.BuildArtifact(bootstrap.ArtifactInput{Component: filter, InputType: reflect.TypeOf(struct{ Tenant int }{}), OutputType: reflect.TypeFor[readerFilter](), DirectViewField: "Rows"})
		if err != nil {
			return nil, err
		}
		reader, err := compiled.ReaderCompilation().NewExecution(bootstrap.ReaderRuntimeConfig{SQL: &dsql.SQLComponent{DB: f.db.DB}})
		if err != nil {
			return nil, err
		}
		components = append(components, &registry.RegisteredComponent{Component: compiled.Component, Input: compiled.Input, Output: compiled.Output, OutputType: reflect.TypeFor[readerFilter](), Reader: reader})
	}

	for _, withResult := range []bool{false, true} {
		path, name := "/read-status/{jobid}", "Status"
		if withResult {
			path, name = "/read-job/{jobid}", "Result"
		}
		endpoint := &spec.Component{Key: spec.Key{Kind: spec.KindComponent, Name: name, Scope: "example.com/results"}, Name: name, Routes: []*spec.Route{{Method: "GET", Path: path, APIKeyHeader: "X-Key", APIKeyValue: "reader-key"}}, Parameters: []*spec.Parameter{{Name: "JobID", Source: spec.BindSource{Kind: "path", Name: "jobid"}, Required: &required}, {Name: "ID", Source: spec.BindSource{Kind: "query", Name: "id"}}, {Name: "Tenant", Source: spec.BindSource{Kind: "query", Name: "tenant"}, Required: &required}, {Name: "Sync", Source: spec.BindSource{Kind: "query", Name: "sync"}}, jwtParam()}}
		compiled, err := bootstrap.BuildArtifact(bootstrap.ArtifactInput{Component: endpoint, InputType: reflect.TypeFor[readerResultInspect](), OutputType: reflect.TypeFor[readerResultOutput](), CodecFactory: f.jwt.Factory})
		if err != nil {
			return nil, err
		}
		components = append(components, &registry.RegisteredComponent{Component: compiled.Component, Input: compiled.Input, Output: compiled.Output, OutputType: reflect.TypeFor[readerResultOutput](), Handler: rhandler.HandlerFunc(func(context.Context, rhandler.Invocation) (any, error) {
			return nil, fmt.Errorf("inspection handler executed")
		})})
		route := gateway.AsyncRoute{Route: spec.RouteRef{Method: "GET", Path: path}, Inspect: &gateway.AsyncInspect{JobID: "JobID", Target: spec.RouteRef{Method: "GET", Path: "/read-results"}, Result: withResult}}
		if withResult {
			route.SyncFlag = "Sync"
		}
		routes = append(routes, route)
	}
	return &application.Build{Components: components, HTTP: gateway.Config{Async: routes, Metrics: &gateway.MetricsConfig{}}}, nil
}

type countedResultReader struct {
	exec.Reader
	calls *atomic.Int32
	gate  func(context.Context) error
}

func (r *countedResultReader) Read(ctx context.Context, input any, binder xhandler.Binder, resolver sqlx.ParameterResolver) (any, error) {
	r.calls.Add(1)
	if r.gate != nil {
		if err := r.gate(ctx); err != nil {
			return nil, err
		}
	}
	return r.Reader.Read(ctx, input, binder, resolver)
}
func (r *countedResultReader) WithRecorder(recorder *observability.Recorder) exec.Reader {
	clone := *r
	clone.Reader = r.Reader.(interface {
		WithRecorder(*observability.Recorder) exec.Reader
	}).WithRecorder(recorder)
	return &clone
}
func (f *readerResultsFixture) start(t *testing.T) {
	manager, err := application.New(nil, application.WithAsync(f.config))
	require.NoError(t, err)
	f.manager = manager
	t.Cleanup(func() { require.NoError(t, manager.Shutdown(context.Background())) })
	require.NoError(t, manager.Reload(context.Background(), application.Request{Revision: 1, Compile: f.compile}))
}
func (f *readerResultsFixture) request(path string) *httptest.ResponseRecorder {
	req := httptest.NewRequest("GET", path, nil)
	req.Header.Set("Authorization", "Bearer "+f.token)
	req.Header.Set("X-Key", "reader-key")
	req.Header.Set("Datly-Show-Metrics", "true")
	response := httptest.NewRecorder()
	f.manager.ServeHTTP(response, req)
	return response
}
func (f *readerResultsFixture) schedule(t *testing.T, key string, tenant int) *xasync.Job {
	t.Helper()
	response := f.request(fmt.Sprintf("/read-results?id=7&tenant=%d&key=%s", tenant, url.QueryEscape(key)))
	require.Equal(t, 200, response.Code, response.Body.String())
	var result readerResultOutput
	require.NoError(t, json.Unmarshal(response.Body.Bytes(), &result))
	require.NotNil(t, result.Job)
	require.Eventually(t, func() bool {
		row, err := f.store.Get(context.Background(), result.Job.ID)
		return err == nil && (row.Status == xasync.StatusDone || row.Status == xasync.StatusError)
	}, 5*time.Second, 5*time.Millisecond)
	row, err := f.store.Get(context.Background(), result.Job.ID)
	require.NoError(t, err)
	failure := ""
	if row.Error != nil {
		failure = *row.Error
	}
	require.Equal(t, xasync.StatusDone, row.Status, failure)
	return result.Job
}
func (f *readerResultsFixture) cacheFiles(t *testing.T) []string {
	t.Helper()
	var result []string
	require.NoError(t, filepath.WalkDir(f.cacheDir, func(path string, entry os.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if !entry.IsDir() && strings.HasSuffix(path, ".json") {
			result = append(result, path)
		}
		return nil
	}))
	return result
}
func TestHTTPCompletedReaderUsesNativeCacheAndCurrentCredentialsSQLite(t *testing.T) {
	f := &readerResultsFixture{}
	f.init(t)
	f.start(t)
	job := f.schedule(t, "one", 1)
	persisted, err := f.store.Get(context.Background(), job.ID)
	require.NoError(t, err)
	require.NotEmpty(t, persisted.SQLQuery)
	require.NoError(t, f.db.ExecStatements(context.Background(), "DROP TABLE result_records"))
	// Freshly issued token; expired stored credentials must never be consulted for
	// a currently authorized foreground result request.
	var state map[string]json.RawMessage
	require.NoError(t, json.Unmarshal([]byte(persisted.State), &state))
	state["JWT"], err = json.Marshal("Bearer " + f.jwt.Sign(t, time.Now().Add(-time.Hour)))
	require.NoError(t, err)
	encoded, err := json.Marshal(state)
	require.NoError(t, err)
	_, err = f.jobDB.DB.Exec("UPDATE APP_JOBS SET State=? WHERE ID=?", string(encoded), job.ID)
	require.NoError(t, err)
	f.token = f.jwt.Sign(t, time.Now().Add(2*time.Hour))
	for _, path := range []string{"/read-results?id=7&tenant=1&key=one", "/read-job/" + job.ID + "?id=7&tenant=1"} {
		response := f.request(path)
		require.Equal(t, 200, response.Code, response.Body.String())
		var output readerResultOutput
		require.NoError(t, json.Unmarshal(response.Body.Bytes(), &output))
		require.Equal(t, "COMPLETE", output.Code)
		require.Len(t, output.Rows, 1)
		require.Equal(t, "cached tenant one", output.Rows[0].Name)
		require.Equal(t, job.ID, output.Job.ID)
		var metric xresponse.Metric
		require.NoError(t, json.Unmarshal([]byte(response.Header().Get("Datly-Metrics-Results")), &metric))
		require.True(t, metric.Executions[0].CacheStats.FoundLazy)
	}
	before := f.reads.Load()
	status := f.request("/read-status/" + job.ID + "?tenant=1")
	require.Equal(t, 200, status.Code, status.Body.String())
	require.Equal(t, before, f.reads.Load())
	require.NotContains(t, status.Body.String(), `"rows"`)
	latest, err := f.store.Get(context.Background(), job.ID)
	require.NoError(t, err)
	require.Equal(t, persisted.EndTime, latest.EndTime)
	require.Equal(t, persisted.SQLQuery, latest.SQLQuery)
}
func TestHTTPCompletedReaderMissExpiryAndSyncRefreshSQLite(t *testing.T) {
	for _, mode := range []string{"missing", "expired", "without lookup"} {
		for _, inspected := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s/inspect=%v", mode, inspected), func(t *testing.T) {
				f := &readerResultsFixture{withoutLookup: mode == "without lookup"}
				f.init(t)
				var notified atomic.Int32
				f.config.Notify = func(context.Context, *xasync.Job) error { notified.Add(1); return nil }
				f.start(t)
				job := f.schedule(t, "reuse", 1)
				require.Eventually(t, func() bool { return notified.Load() == 1 }, time.Second, time.Millisecond)
				original, err := f.store.Get(context.Background(), job.ID)
				require.NoError(t, err)
				files := f.cacheFiles(t)
				require.NotEmpty(t, files)
				for _, path := range files {
					if mode != "expired" {
						require.NoError(t, os.Remove(path))
						continue
					}
					payload, err := os.ReadFile(path)
					require.NoError(t, err)
					boundary := strings.IndexByte(string(payload), '\n')
					require.Positive(t, boundary)
					var meta cache.Meta
					require.NoError(t, json.Unmarshal(payload[:boundary], &meta))
					meta.ExpiryTimeMs = 1
					header, err := json.Marshal(meta)
					require.NoError(t, err)
					require.NoError(t, os.WriteFile(path, append(header, payload[boundary:]...), 0600))
				}
				path := "/read-results?id=7&tenant=1&key=reuse"
				if inspected {
					path = "/read-job/" + job.ID + "?id=7&tenant=1"
				}
				// DONE without SyncFlag follows the ordinary native cache miss/write path.
				require.NoError(t, f.db.ExecStatements(context.Background(), "UPDATE result_records SET name='default database fallback' WHERE tenant=1"))
				fallback := f.request(path)
				require.Equal(t, 200, fallback.Code, fallback.Body.String())
				var output readerResultOutput
				require.NoError(t, json.Unmarshal(fallback.Body.Bytes(), &output))
				require.Len(t, output.Rows, 1)
				require.Equal(t, "default database fallback", output.Rows[0].Name)
				require.Equal(t, job.ID, output.Job.ID)
				require.Equal(t, "COMPLETE", output.Code)
				var metric xresponse.Metric
				require.NoError(t, json.Unmarshal([]byte(fallback.Header().Get("Datly-Metrics-Results")), &metric))
				require.Equal(t, cache.TypeWrite, metric.Executions[0].CacheStats.Type)
				require.False(t, metric.Executions[0].CacheStats.FoundLazy || metric.Executions[0].CacheStats.FoundWarmup)
				require.NotEmpty(t, f.cacheFiles(t))
				require.NoError(t, f.db.ExecStatements(context.Background(), "UPDATE result_records SET name='explicit sync refresh' WHERE tenant=1"))
				cached := f.request(path)
				require.Equal(t, 200, cached.Code, cached.Body.String())
				require.Contains(t, cached.Body.String(), "default database fallback")
				require.NoError(t, json.Unmarshal([]byte(cached.Header().Get("Datly-Metrics-Results")), &metric))
				require.True(t, metric.Executions[0].CacheStats.FoundLazy)
				refreshed := f.request(path + "&sync=true")
				require.Equal(t, 200, refreshed.Code, refreshed.Body.String())
				require.Contains(t, refreshed.Body.String(), "explicit sync refresh")
				require.NoError(t, json.Unmarshal([]byte(refreshed.Header().Get("Datly-Metrics-Results")), &metric))
				require.Equal(t, cache.TypeWrite, metric.Executions[0].CacheStats.Type)
				require.NoError(t, f.db.ExecStatements(context.Background(), "DROP TABLE result_records"))
				cached = f.request(path)
				require.Equal(t, 200, cached.Code, cached.Body.String())
				require.Contains(t, cached.Body.String(), "explicit sync refresh")
				before := f.reads.Load()
				status := f.request("/read-status/" + job.ID + "?tenant=1")
				require.Equal(t, 200, status.Code, status.Body.String())
				require.Equal(t, before, f.reads.Load())
				latest, err := f.store.Get(context.Background(), job.ID)
				require.NoError(t, err)
				require.Equal(t, original, latest, "result reads must not rewrite terminal history")
				require.EqualValues(t, 1, notified.Load(), "result reads must not notify completion again")
			})
		}
	}
}
func TestHTTPCompletedReaderRevocationTenantAndQueryGuardSQLite(t *testing.T) {
	f := &readerResultsFixture{}
	f.init(t)
	f.start(t)
	one := f.schedule(t, "one", 1)
	f.allowedTenant.Store(2)
	f.schedule(t, "two", 2)
	// Even a populated, currently authorized other-tenant cache cannot be returned
	// under the first job's durable execution identity.
	different := f.request("/read-results?id=7&tenant=2&key=one")
	require.Equal(t, 409, different.Code, different.Body.String())
	require.NotContains(t, different.Body.String(), "cached tenant two")
	different = f.request("/read-job/" + one.ID + "?id=7&tenant=2&sync=true")
	require.Equal(t, 409, different.Code, different.Body.String())
	for _, path := range f.cacheFiles(t) {
		require.NoError(t, os.Remove(path))
	}
	require.NoError(t, f.db.ExecStatements(context.Background(), "DROP TABLE result_records"))
	different = f.request("/read-job/" + one.ID + "?id=7&tenant=2")
	require.Equal(t, 409, different.Code, different.Body.String())
	require.Empty(t, f.cacheFiles(t), "rejected query must not admit a cache writer")
	f.allowedTenant.Store(1)
	f.revoked.Store(true)
	before := f.reads.Load()
	denied := f.request("/read-results?id=7&tenant=1&key=one")
	require.Equal(t, 403, denied.Code, denied.Body.String())
	require.Equal(t, before, f.reads.Load())
	f.revoked.Store(false)
	f.token = f.jwt.Sign(t, time.Now().Add(-time.Hour))
	denied = f.request("/read-job/" + one.ID + "?id=7&tenant=1")
	require.Equal(t, 401, denied.Code, denied.Body.String())
	require.Equal(t, before, f.reads.Load())
	f.token = f.jwt.Sign(t, time.Now().Add(time.Hour))
	f.sql += " ORDER BY name"
	require.NoError(t, f.manager.Reload(context.Background(), application.Request{Revision: 2, Compile: f.compile}))
	incompatible := f.request("/read-results?id=7&tenant=1&key=one")
	require.Equal(t, 409, incompatible.Code, incompatible.Body.String())
}
