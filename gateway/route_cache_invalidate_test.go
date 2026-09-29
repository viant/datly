package gateway

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"

	_ "github.com/mattn/go-sqlite3"
	"github.com/stretchr/testify/require"
	"github.com/viant/datly/gateway/runtime/meta"
	"github.com/viant/datly/repository"
	"github.com/viant/datly/repository/contract"
	"github.com/viant/datly/repository/path"
	"github.com/viant/datly/repository/version"
	viewreader "github.com/viant/datly/service/reader"
	"github.com/viant/datly/view"
	"github.com/viant/sqlx/io/read"
	sqlcache "github.com/viant/sqlx/io/read/cache"
)

func invalidationProvider(t *testing.T) (*repository.Provider, []*view.View) {
	t.Helper()
	root := t.TempDir()
	config := fmt.Sprintf(`CacheProviders:
  - Name: local
    Location: %s/cache/${View.Name}
    TimeToLiveMs: 60000
Connectors:
  - Name: db
    Driver: sqlite3
    DSN: %s/data.db
Views:
  - Name: orders
    Table: items
    Connector:
      Ref: db
    Columns:
      - Name: id
        DataType: int
    Cache:
      Ref: local
      Warmup:
        IndexColumn: id
  - Name: details
    Table: items
    Connector:
      Ref: db
    Columns:
      - Name: id
        DataType: int
    Cache:
      Ref: local
`, root, root)
	file := filepath.Join(root, "resource.yaml")
	require.NoError(t, os.WriteFile(file, []byte(config), 0600))
	resource, err := view.NewResourceFromURL(context.Background(), file, nil, nil)
	require.NoError(t, err)
	views := []*view.View{resource.Views[0], resource.Views[1]}
	views[0].With = []*view.Relation{{Of: &view.ReferenceView{View: *views[1]}}}
	provider := repository.NewProvider(*contract.NewPath(http.MethodGet, "/v1/api/orders"), &version.Control{}, func(context.Context, ...repository.Option) (*repository.Component, error) {
		return &repository.Component{Path: *contract.NewPath(http.MethodGet, "/v1/api/orders"), View: views[0]}, nil
	})
	return provider, views
}
func invalidationRouter() *Router {
	return &Router{config: &Config{ExposableConfig: ExposableConfig{APIPrefix: "/v1/api", Meta: meta.Config{CacheInvalidateURI: meta.CacheInvalidateURI}}}}
}
func TestCacheInvalidationEndpoint(t *testing.T) {
	provider, _ := invalidationProvider(t)
	route := invalidationRouter().NewCacheInvalidationRoute("/v1/api/cache/invalidate/orders", provider)
	for _, tc := range []struct {
		body          string
		status, count int
		scope         string
	}{
		{``, 200, 2, "all"},
		{`{"scope":"lazy"}`, 200, 2, "lazy"},
		{`{"scope":"warmup","view":"orders"}`, 200, 1, "warmup"},
		{`{"scope":"lazy","view":"details"}`, 200, 1, "lazy"},
		{`{"view":"missing"}`, 404, 0, ""},
		{`{"scope":"unknown"}`, 400, 0, ""},
		{`{"scpoe":"all"}`, 400, 0, ""},
		{`{} {}`, 400, 0, ""},
	} {
		t.Run(tc.body, func(t *testing.T) {
			response := httptest.NewRecorder()
			route.Handle(response, httptest.NewRequest(http.MethodPost, route.URI(), strings.NewReader(tc.body)))
			require.Equal(t, tc.status, response.Code, response.Body.String())
			result := cacheInvalidationResponse{}
			require.NoError(t, json.Unmarshal(response.Body.Bytes(), &result))
			require.Len(t, result.Invalidated, tc.count)
			for _, entry := range result.Invalidated {
				require.NotEmpty(t, entry.Generation)
				require.Equal(t, tc.scope, string(entry.Scope))
			}
		})
	}
}
func TestCacheInvalidationRejectsUnauthorizedBeforeMutation(t *testing.T) {
	provider, views := invalidationProvider(t)
	route := invalidationRouter().NewCacheInvalidationRoute("/v1/api/cache/invalidate/orders", provider)
	route.ApiKeys = []*path.APIKey{{Header: "X-API-Key", Value: "secret"}}
	before, err := filepath.Glob(views[0].Cache.Location + "/.datly-generations/*/all")
	require.NoError(t, err)
	require.Empty(t, before)
	response := httptest.NewRecorder()
	status := route.Handle(response, httptest.NewRequest(http.MethodPost, route.URI(), nil))
	require.Equal(t, http.StatusForbidden, status)
	require.Equal(t, http.StatusForbidden, response.Code)
	// The request must not create any generation marker.
	location, err := views[0].Cache.ExpandedLocation(views[0])
	require.NoError(t, err)
	after, err := filepath.Glob(location + "/.datly-generations/*/all")
	require.NoError(t, err)
	require.Empty(t, after)
	req := httptest.NewRequest(http.MethodPost, route.URI(), nil)
	req.Header.Set("X-API-Key", "secret")
	response = httptest.NewRecorder()
	route.Handle(response, req)
	require.Equal(t, http.StatusOK, response.Code, response.Body.String())
	after, err = filepath.Glob(location + "/.datly-generations/*/all")
	require.NoError(t, err)
	require.Len(t, after, 1)
}
func TestAppendCacheInvalidationRoute(t *testing.T) {
	r := invalidationRouter()
	routes := r.appendCacheInvalidationRoute(nil, &path.Path{Path: *contract.NewPath(http.MethodGet, "/v1/api/orders")}, nil)
	require.Len(t, routes, 1)
	require.Equal(t, "/v1/api/cache/invalidate/orders", routes[0].URI())
	require.Equal(t, http.MethodPost, routes[0].Path.Method)
	require.Empty(t, r.appendCacheInvalidationRoute(nil, &path.Path{Path: *contract.NewPath(http.MethodPut, "/v1/api/orders")}, nil))
}

func TestCacheInvalidationEndpointRetiresCachedData(t *testing.T) {
	provider, views := invalidationProvider(t)
	db, err := views[0].Db()
	require.NoError(t, err)
	_, err = db.Exec("CREATE TABLE items(id INTEGER); INSERT INTO items VALUES(1)")
	require.NoError(t, err)
	lookup := func() (int, bool) {
		service, err := views[0].Cache.Service()
		require.NoError(t, err)
		type row struct{ ID int }
		stats := &sqlcache.Stats{}
		reader, err := read.New(context.Background(), db, "SELECT id FROM items", func() any { return &row{} }, read.WithCache(service), read.WithCacheStats(stats))
		require.NoError(t, err)
		defer func() {
			if stmt := reader.Stmt(); stmt != nil {
				_ = stmt.Close()
			}
		}()
		value := 0
		require.NoError(t, reader.QueryAll(context.Background(), func(v any) error { value = v.(*row).ID; return nil }))
		return value, stats.FoundAny()
	}
	value, hit := lookup()
	require.Equal(t, 1, value)
	require.False(t, hit)
	_, err = db.Exec("UPDATE items SET id=2")
	require.NoError(t, err)
	value, hit = lookup()
	require.Equal(t, 1, value)
	require.True(t, hit)
	route := invalidationRouter().NewCacheInvalidationRoute("/v1/api/cache/invalidate/orders", provider)
	response := httptest.NewRecorder()
	route.Handle(response, httptest.NewRequest(http.MethodPost, route.URI(), strings.NewReader(`{"view":"orders","scope":"lazy"}`)))
	require.Equal(t, http.StatusOK, response.Code, response.Body.String())
	value, hit = lookup()
	require.Equal(t, 2, value)
	require.False(t, hit)
}

func TestCacheWarmupFailureReturnsHTTPError(t *testing.T) {
	provider, _ := invalidationProvider(t)
	status, body := invalidationRouter().handleCacheWarmupWithErr(context.Background(), []*repository.Provider{provider})
	require.Equal(t, http.StatusInternalServerError, status, string(body))
	var response struct {
		Status string `json:"status"`
		Error  string `json:"error"`
	}
	require.NoError(t, json.Unmarshal(body, &response))
	require.Equal(t, "error", response.Status)
	require.NotEmpty(t, response.Error)
}

func TestCacheWarmupHTTPInvalidationAndRepopulation(t *testing.T) {
	ctx := context.Background()
	provider, views := invalidationProvider(t)
	db, err := views[0].Db()
	require.NoError(t, err)
	_, err = db.Exec("CREATE TABLE items(id INTEGER); INSERT INTO items VALUES(1)")
	require.NoError(t, err)
	r := invalidationRouter()
	status, body := r.handleCacheWarmupWithErr(ctx, []*repository.Provider{provider})
	require.Equal(t, http.StatusOK, status, string(body))
	inputs, err := views[0].Cache.GenerateCacheInput(ctx)
	require.NoError(t, err)
	require.NotEmpty(t, inputs)
	identity, err := viewreader.NewBuilder().CacheSQL(ctx, views[0], inputs[0].Selector)
	require.NoError(t, err)
	lookup := func() ([]int, bool) {
		service, err := views[0].Cache.Service()
		require.NoError(t, err)
		matcher := &sqlcache.ParmetrizedQuery{SQL: "SELECT id FROM items WHERE id = ?", Args: []any{1}, IdentitySQL: identity.SQL, IdentityArgs: identity.Args, By: "id", In: []any{1}, Limit: 10}
		type row struct{ ID int }
		stats := &sqlcache.Stats{}
		reader, err := read.New(ctx, db, matcher.SQL, func() any { return &row{} }, read.WithCache(service), read.WithInMatcher(matcher), read.WithCacheStats(stats))
		require.NoError(t, err)
		defer func() {
			if stmt := reader.Stmt(); stmt != nil {
				_ = stmt.Close()
			}
		}()
		var ids []int
		require.NoError(t, reader.QueryAll(ctx, func(value any) error { ids = append(ids, value.(*row).ID); return nil }, 1))
		return ids, stats.FoundWarmup
	}
	ids, hit := lookup()
	require.Equal(t, []int{1}, ids)
	require.True(t, hit)
	_, err = db.Exec("DELETE FROM items")
	require.NoError(t, err)
	ids, hit = lookup()
	require.Equal(t, []int{1}, ids)
	require.True(t, hit)
	route := r.NewCacheInvalidationRoute("/v1/api/cache/invalidate/orders", provider)
	response := httptest.NewRecorder()
	route.Handle(response, httptest.NewRequest(http.MethodPost, route.URI(), strings.NewReader(`{"view":"orders","scope":"warmup"}`)))
	require.Equal(t, http.StatusOK, response.Code, response.Body.String())
	status, body = r.handleCacheWarmupWithErr(ctx, []*repository.Provider{provider})
	require.Equal(t, http.StatusOK, status, string(body))
	ids, hit = lookup()
	require.Empty(t, ids)
	require.True(t, hit)
}

func TestCacheInvalidationHandlesRecursiveViews(t *testing.T) {
	provider, views := invalidationProvider(t)
	reference := &view.ReferenceView{View: *views[0]}
	relation := &view.Relation{Of: reference}
	reference.View.With = []*view.Relation{relation}
	views[0].With = []*view.Relation{relation}
	route := invalidationRouter().NewCacheInvalidationRoute("/v1/api/cache/invalidate/orders", provider)
	response := httptest.NewRecorder()
	require.Equal(t, http.StatusOK, route.Handle(response, httptest.NewRequest(http.MethodPost, route.URI(), nil)))
	var result cacheInvalidationResponse
	require.NoError(t, json.Unmarshal(response.Body.Bytes(), &result))
	require.Len(t, result.Invalidated, 1)
}
func TestCacheWarmupRouteReportsFailureStatus(t *testing.T) {
	provider, _ := invalidationProvider(t)
	route := invalidationRouter().NewWarmupRoute("/v1/api/cache/warmup/orders", provider)
	response := httptest.NewRecorder()
	status := route.Handle(response, httptest.NewRequest(http.MethodPost, route.URI(), nil))
	require.Equal(t, http.StatusInternalServerError, status)
	require.Equal(t, status, response.Code)
}
