package gateway

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/viant/authz"
	"github.com/viant/datly/repository"
	"github.com/viant/gmetric"
)

func TestAuthorizationFrameworkRoutesAndFailedRebuild(t *testing.T) {
	ctx := context.Background()
	repo, err := repository.New(ctx, repository.WithComponentURL(""), repository.WithNoPlugin(), repository.WithRefreshDisabled(true))
	require.NoError(t, err)
	config := authorizationFixture()
	require.NoError(t, config.Init(ctx))
	calls := 0
	config.AuthorizationProviders = map[string]AuthorizationProvider{"identity": func(ctx context.Context, _ *http.Request) (context.Context, authz.Facts, error) {
		calls++
		return ctx, authz.Facts{}, ErrUnauthenticated
	}}
	router, err := NewRouter(ctx, repo, config, gmetric.New(), http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(200) }), nil)
	require.NoError(t, err)
	for _, path := range []string{config.Meta.ConfigURI, config.Meta.OpenApiURI, config.Meta.DocURI, config.Meta.MetricURI, "/unknown"} {
		w := httptest.NewRecorder()
		router.Handle(w, httptest.NewRequest("GET", path, nil))
		require.Equal(t, 401, w.Code, path)
	}
	require.Equal(t, 5, calls)
	config.Authorization.DefaultPolicy = "missing"
	_, err = NewRouter(ctx, repo, config, gmetric.New(), nil, nil)
	require.Error(t, err)
	w := httptest.NewRecorder()
	router.Handle(w, httptest.NewRequest("GET", "/healthz", nil))
	require.Equal(t, 200, w.Code)
	w = httptest.NewRecorder()
	router.Handle(w, httptest.NewRequest("GET", config.Meta.ConfigURI, nil))
	require.Equal(t, 401, w.Code)
}

func BenchmarkAuthorizationHTTP(b *testing.B) {
	for _, enabled := range []bool{false, true} {
		name := "disabled"
		if enabled {
			name = "protected"
		}
		b.Run(name, func(b *testing.B) {
			config := authorizationFixture()
			config.Authorization.Exclude = nil
			config.Authorization.Policies["reader"] = authz.Policy{Mode: "protected", Rule: &authz.Rule{Kind: "subject", Value: "*"}}
			config.AuthorizationProviders = map[string]AuthorizationProvider{"identity": func(ctx context.Context, _ *http.Request) (context.Context, authz.Facts, error) {
				return ctx, authz.Facts{Subject: "user", Tenant: "tenant", Issuer: "fixture", ValidUntil: time.Now().Add(time.Hour)}, nil
			}}
			if !enabled {
				config.Authorization = nil
			}
			ctx := context.Background()
			if err := config.Init(ctx); err != nil {
				b.Fatal(err)
			}
			repo, err := repository.New(ctx, repository.WithComponentURL(""), repository.WithNoPlugin(), repository.WithRefreshDisabled(true))
			if err != nil {
				b.Fatal(err)
			}
			router, err := NewRouter(ctx, repo, config, gmetric.New(), http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(200) }), nil)
			if err != nil {
				b.Fatal(err)
			}
			request := httptest.NewRequest("GET", "/healthz", nil)
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				w := httptest.NewRecorder()
				router.Handle(w, request)
				if w.Code != 200 {
					b.Fatal(w.Code)
				}
			}
		})
	}
}
