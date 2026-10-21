package gateway

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/viant/datly/gateway/runtime/meta"
	"github.com/viant/datly/gateway/warmup"
	"github.com/viant/datly/repository"
	"github.com/viant/datly/repository/contract"
	"github.com/viant/datly/repository/path"
	"github.com/viant/datly/repository/version"
	"github.com/viant/datly/view"
)

func TestRouterAppendCacheWarmupRoute_GET(t *testing.T) {
	router := &Router{
		config: &Config{
			ExposableConfig: ExposableConfig{
				APIPrefix: "/v1/api",
				Meta:      meta.Config{CacheWarmURI: "/v1/api/cache/warmup"},
			},
		},
	}
	aPath := &path.Path{Path: *contract.NewPath(http.MethodGet, "/v1/api/order")}

	routes := router.appendCacheWarmupRoute(nil, aPath, nil)

	require.Len(t, routes, 1)
	require.Equal(t, RouteWarmupKind, routes[0].Kind)
	require.Equal(t, http.MethodPost, routes[0].Path.Method)
	require.Equal(t, "/v1/api/cache/warmup/order", routes[0].Path.URI)
}

func TestRouterAppendCacheWarmupRoute_NonGET(t *testing.T) {
	router := &Router{
		config: &Config{
			ExposableConfig: ExposableConfig{
				APIPrefix: "/v1/api",
				Meta:      meta.Config{CacheWarmURI: "/v1/api/cache/warmup"},
			},
		},
	}
	aPath := &path.Path{Path: *contract.NewPath(http.MethodPost, "/v1/api/order")}

	routes := router.appendCacheWarmupRoute(nil, aPath, nil)

	require.Empty(t, routes)
}

func TestRouterInternalGETAllowsWarmupRouteOnly(t *testing.T) {
	aPath := &path.Path{
		Path:     *contract.NewPath(http.MethodGet, "/v1/api/internal/order"),
		Internal: true,
	}
	router := newWarmupTestRouter(t, aPath)

	_, err := router.Match(http.MethodGet, "/v1/api/internal/order", nil)
	require.Error(t, err)

	route, err := router.Match(http.MethodPost, "/v1/api/cache/warmup/internal/order", nil)
	require.NoError(t, err)
	require.Equal(t, RouteWarmupKind, route.Kind)
}

func TestRouterInternalPOSTDoesNotAllowWarmupRoute(t *testing.T) {
	aPath := &path.Path{
		Path:     *contract.NewPath(http.MethodPost, "/v1/api/internal/order"),
		Internal: true,
	}
	router := newWarmupTestRouter(t, aPath)

	_, err := router.Match(http.MethodPost, "/v1/api/internal/order", nil)
	require.Error(t, err)

	_, err = router.Match(http.MethodPost, "/v1/api/cache/warmup/internal/order", nil)
	require.Error(t, err)
}

func TestRouterHandleCacheWarmupWithErr_NoCacheViews(t *testing.T) {
	router := &Router{}
	provider := repository.NewProvider(
		*contract.NewPath(http.MethodGet, "/v1/api/order"),
		&version.Control{},
		func(ctx context.Context, opts ...repository.Option) (*repository.Component, error) {
			return &repository.Component{
				Path: *contract.NewPath(http.MethodGet, "/v1/api/order"),
				View: &view.View{Name: "order"},
			}, nil
		},
	)

	statusCode, body := router.handleCacheWarmupWithErr(context.Background(), []*repository.Provider{provider})

	require.Equal(t, http.StatusOK, statusCode)
	response := &warmup.Response{}
	require.NoError(t, json.Unmarshal(body, response))
	require.Equal(t, "ok", response.Status)
	require.Empty(t, response.PreCached)
}

func TestRouterHandleCacheWarmupWithErr_DetachesRequestContext(t *testing.T) {
	router := &Router{}
	provider := repository.NewProvider(
		*contract.NewPath(http.MethodGet, "/v1/api/order"),
		&version.Control{},
		func(ctx context.Context, opts ...repository.Option) (*repository.Component, error) {
			if err := ctx.Err(); err != nil {
				return nil, fmt.Errorf("warmup context should not be request-canceled: %w", err)
			}
			return &repository.Component{
				Path: *contract.NewPath(http.MethodGet, "/v1/api/order"),
				View: &view.View{Name: "order"},
			}, nil
		},
	)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	statusCode, body := router.handleCacheWarmupWithErr(ctx, []*repository.Provider{provider})

	require.Equal(t, http.StatusOK, statusCode, string(body))
}

func newWarmupTestRouter(t *testing.T, routes ...*path.Path) *Router {
	t.Helper()
	ctx := context.Background()
	repo, err := repository.New(ctx, repository.WithComponentURL(""), repository.WithNoPlugin())
	require.NoError(t, err)

	item := &path.Item{Paths: routes}
	repo.Container().Items = []*path.Item{item}

	providers := make([]*repository.Provider, 0, len(routes))
	for _, routePath := range routes {
		routePath := routePath
		component, err := repository.NewComponent(&routePath.Path, repository.WithView(&view.View{Name: "order"}))
		require.NoError(t, err)
		providers = append(providers, repository.NewProvider(routePath.Path, &version.Control{}, func(ctx context.Context, opts ...repository.Option) (*repository.Component, error) {
			return component, nil
		}))
	}
	repo.Registry().SetProviders(providers)

	router, err := NewRouter(ctx, repo, &Config{
		ExposableConfig: ExposableConfig{
			APIPrefix: "/v1/api",
			Meta:      meta.Config{CacheWarmURI: "/v1/api/cache/warmup"},
		},
	}, nil, nil, nil)
	require.NoError(t, err)
	return router
}
