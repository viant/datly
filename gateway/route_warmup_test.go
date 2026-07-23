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
