package gateway

import (
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/golang-jwt/jwt/v5"
	"github.com/stretchr/testify/require"
	"github.com/viant/authz"
	"github.com/viant/authz/oauth"
	"github.com/viant/cloudless/gateway/matcher"
	"github.com/viant/datly/repository"
	"github.com/viant/datly/repository/contract"
	"github.com/viant/gmetric"
)

func authorizationFixture() *Config {
	c := &Config{}
	c.Meta.StatusURI = "/healthz"
	c.Authorization = &AuthorizationConfig{Provider: "identity", DefaultPolicy: "reader", Policies: map[string]authz.Policy{
		"reader":   {Mode: "protected", Rule: &authz.Rule{Kind: "all", Rules: []authz.Rule{{Kind: "role", Value: "reader"}, {Kind: "not", Rules: []authz.Rule{{Kind: "role", Value: "suspended"}}}}}},
		"reviewer": {Mode: "protected", Rule: &authz.Rule{Kind: "role", Value: "reviewer"}},
	}, Overrides: []AuthorizationOverride{{Match: AuthorizationMatch{Method: "GET", Path: "/items/{id}"}, Policy: "reviewer"}}, Exclude: []AuthorizationMatch{{Method: "GET", Path: "/healthz"}}}
	return c
}

func authorizationRouter(t *testing.T, c *Config, executed *atomic.Int32) *Router {
	t.Helper()
	require.NoError(t, c.Init(context.Background()))
	repo, err := repository.New(context.Background(), repository.WithComponentURL(""), repository.WithNoPlugin(), repository.WithRefreshDisabled(true))
	require.NoError(t, err)
	r, err := NewRouter(context.Background(), repo, c, gmetric.New(), http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(200) }), nil)
	require.NoError(t, err)
	// Keep framework routes from the real router; add test business handlers through
	// the same matcher, without substituting policy selection or HTTP dispatch.
	routes := []matcher.Matchable{}
	for _, path := range []string{"/records", "/items/{id}", "/static/app.js", "/admin/cache"} {
		routes = append(routes, &Route{Path: contract.NewPath("GET", path), Handler: func(_ context.Context, w http.ResponseWriter, req *http.Request) {
			executed.Add(1)
			require.NotNil(t, AuthorizationFacts(req.Context()))
			w.WriteHeader(200)
		}})
	}
	routes = append(routes, r.NewStatusRoute(), r.NewConfigRoute())
	r.routeMatcher = matcher.NewMatcher(routes)
	return r
}

func TestAuthorizationMockHTTPRoles(t *testing.T) {
	public, private, err := ed25519.GenerateKey(rand.Reader)
	require.NoError(t, err)
	for _, tc := range []struct {
		name, method, path                      string
		roles                                   []string
		identityStatus, want, calls, executions int
		missing, expired, mismatch              bool
	}{
		{name: "public health", method: "GET", path: "/healthz", want: 200},
		{name: "reader", method: "GET", path: "/records", roles: []string{"reader"}, want: 200, calls: 1, executions: 1},
		{name: "positive and negative", method: "GET", path: "/records", roles: []string{"reader", "suspended"}, want: 403, calls: 1},
		{name: "wrong role", method: "GET", path: "/records", roles: []string{"reviewer"}, want: 403, calls: 1},
		{name: "template override", method: "GET", path: "/items/42", roles: []string{"reviewer"}, want: 200, calls: 1, executions: 1},
		{name: "override replaces default", method: "GET", path: "/items/42", roles: []string{"reader"}, want: 403, calls: 1},
		{name: "missing credential", method: "GET", path: "/records", missing: true, want: 401},
		{name: "expired credential", method: "GET", path: "/records", expired: true, want: 401},
		{name: "provider unavailable", method: "GET", path: "/records", identityStatus: 503, want: 503, calls: 1},
		{name: "identity mismatch", method: "GET", path: "/records", roles: []string{"reader"}, mismatch: true, want: 401, calls: 1},
		{name: "wrong health method", method: "POST", path: "/healthz", missing: true, want: 401},
		{name: "health child", method: "GET", path: "/healthz/child", missing: true, want: 401},
		{name: "encoded health alias", method: "GET", path: "/%68ealthz", missing: true, want: 401},
		{name: "unknown denied", method: "GET", path: "/unknown", roles: []string{}, want: 403, calls: 1},
		{name: "unknown authenticated", method: "GET", path: "/unknown", roles: []string{"reader"}, want: 404, calls: 1},
		{name: "static protected", method: "GET", path: "/static/app.js", missing: true, want: 401},
		{name: "admin protected", method: "GET", path: "/admin/cache", missing: true, want: 401},
		{name: "config protected", method: "GET", path: "/v1/api/meta/config", missing: true, want: 401},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var calls, executions atomic.Int32
			identity := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				calls.Add(1)
				require.True(t, strings.HasPrefix(r.Header.Get("Authorization"), "Bearer "))
				if tc.identityStatus != 0 {
					http.Error(w, "sensitive upstream diagnostic", tc.identityStatus)
					return
				}
				subject := "user"
				if tc.mismatch {
					subject = "different"
				}
				json.NewEncoder(w).Encode(map[string]any{"sub": subject, "account": 42, "roles": tc.roles})
			}))
			defer identity.Close()
			providers, err := oauth.NewHTTPProviders(context.Background(), oauth.HTTPIdentityConfig{Kind: "oidc-userinfo", Issuer: identity.URL, Audience: "api", Algorithms: []string{"EdDSA"}, UserInfoURL: identity.URL, Keyfunc: func(*jwt.Token) (any, error) { return public, nil }, JWKSRefreshInterval: "1m", FactLease: "1m", UserInfoCacheTTL: "0s", TenantForAllAccounts: "tenant", Mapping: oauth.HTTPIdentityMapping{Subject: "sub", AccountID: "account", JWTAccountID: "account_id", Roles: "roles"}}, nil)
			require.NoError(t, err)
			config := authorizationFixture()
			config.AuthorizationProviders = map[string]AuthorizationProvider{"identity": func(ctx context.Context, r *http.Request) (context.Context, authz.Facts, error) {
				if r.Header.Get("Authorization") == "" {
					return ctx, authz.Facts{}, ErrUnauthenticated
				}
				ctx = oauth.WithBearer(ctx, strings.TrimPrefix(r.Header.Get("Authorization"), "Bearer "))
				facts, err := providers.Identity.Resolve(ctx)
				if errors.Is(err, authz.ErrDenied) {
					err = fmt.Errorf("%w: %w", ErrUnauthenticated, err)
				}
				return ctx, facts, err
			}}
			router := authorizationRouter(t, config, &executions)
			server := httptest.NewServer(http.HandlerFunc(router.Handle))
			defer server.Close()
			request, err := http.NewRequest(tc.method, server.URL+tc.path, nil)
			require.NoError(t, err)
			if !tc.missing {
				expiry := time.Now().Add(time.Hour)
				if tc.expired {
					expiry = time.Now().Add(-time.Hour)
				}
				token, err := jwt.NewWithClaims(jwt.SigningMethodEdDSA, jwt.MapClaims{"iss": identity.URL, "aud": "api", "sub": "user", "account_id": 42, "exp": expiry.Unix()}).SignedString(private)
				require.NoError(t, err)
				request.Header.Set("Authorization", "Bearer "+token)
			}
			response, err := server.Client().Do(request)
			require.NoError(t, err)
			defer response.Body.Close()
			body, readErr := io.ReadAll(response.Body)
			require.NoError(t, readErr)
			require.NotContains(t, string(body), "sensitive upstream diagnostic")
			require.Equal(t, tc.want, response.StatusCode, string(body))
			require.Equal(t, int32(tc.calls), calls.Load())
			require.Equal(t, int32(tc.executions), executions.Load())
		})
	}
}
