package http

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/viant/authz"
	dexec "github.com/viant/datly/exec"
	druntime "github.com/viant/datly/runtime"
	"github.com/viant/datly/spec"
)

func TestAuthorizationHTTPBoundary(t *testing.T) {
	for _, tc := range []struct {
		name, method, path string
		role               string
		providerErr        error
		want, calls        int
	}{
		{name: "health excluded", method: "GET", path: "/healthz", want: 200},
		{name: "health other method protected", method: "POST", path: "/healthz", providerErr: ErrUnauthenticated, want: 401, calls: 1},
		{name: "health child protected", method: "GET", path: "/healthz/child", providerErr: ErrUnauthenticated, want: 401, calls: 1},
		{name: "encoded health protected", method: "GET", path: "/%68ealthz", providerErr: ErrUnauthenticated, want: 401, calls: 1},
		{name: "unknown protected", method: "GET", path: "/missing", role: "reader", want: 404, calls: 1},
		{name: "override denied", method: "GET", path: "/review", role: "reader", want: 403, calls: 1},
		{name: "override permitted", method: "GET", path: "/review", role: "reviewer", want: 404, calls: 1},
		{name: "metrics protected", method: "GET", path: "/v1/api/meta/metric", providerErr: ErrUnauthenticated, want: 401, calls: 1},
		{name: "docs protected", method: "GET", path: "/v1/api/meta/openapi", providerErr: ErrUnauthenticated, want: 401, calls: 1},
		{name: "preflight protected", method: "OPTIONS", path: "/review", providerErr: ErrUnauthenticated, want: 401, calls: 1},
		{name: "unavailable", method: "GET", path: "/review", providerErr: errors.New("secret upstream payload"), want: 503, calls: 1},
		{name: "canceled", method: "GET", path: "/review", providerErr: context.Canceled, want: 408, calls: 1},
		{name: "deadline", method: "GET", path: "/review", providerErr: context.DeadlineExceeded, want: 504, calls: 1},
	} {
		t.Run(tc.name, func(t *testing.T) {
			calls := 0
			config := authorizationFixture()
			config.AuthorizationProviders = map[string]AuthorizationProvider{"identity": func(ctx context.Context, r *http.Request) (context.Context, authz.Facts, error) {
				calls++
				return ctx, authz.Facts{Subject: "user", Tenant: "tenant", Issuer: "issuer", Roles: []string{tc.role}, ValidUntil: time.Now().Add(time.Minute)}, tc.providerErr
			}}
			h := statusHandler(t, config, time.Now())
			w := httptest.NewRecorder()
			h.ServeHTTP(w, httptest.NewRequest(tc.method, tc.path, nil))
			require.Equal(t, tc.want, w.Code, w.Body.String())
			require.Equal(t, tc.calls, calls)
			require.NotContains(t, w.Body.String(), "secret upstream payload")
		})
	}
}

func authorizationFixture() Config {
	return Config{Meta: Meta{StatusURI: "/healthz"}, Authorization: &AuthorizationConfig{
		Provider: "identity", DefaultPolicy: "default",
		Policies: map[string]authz.Policy{
			"default": {Mode: "protected", Rule: &authz.Rule{Kind: "subject", Value: "*"}},
			"review":  {Mode: "protected", Rule: &authz.Rule{Kind: "role", Value: "reviewer"}},
		},
		Overrides: []AuthorizationOverride{{Match: spec.RouteRef{Method: "GET", Path: "/review"}, Policy: "review"}},
		Exclude:   []spec.RouteRef{{Method: "GET", Path: "/healthz"}},
	}, AuthorizationProviders: map[string]AuthorizationProvider{"identity": func(ctx context.Context, _ *http.Request) (context.Context, authz.Facts, error) {
		return ctx, authz.Facts{}, ErrUnauthenticated
	}}}
}

func TestAuthorizationInvalidBuild(t *testing.T) {
	for _, tc := range []struct {
		name   string
		change func(*Config)
	}{
		{"missing provider", func(c *Config) { c.AuthorizationProviders = nil }},
		{"missing default", func(c *Config) { c.Authorization.DefaultPolicy = "" }},
		{"unknown policy", func(c *Config) { c.Authorization.Overrides[0].Policy = "unknown" }},
		{"conflict", func(c *Config) {
			c.Authorization.Exclude = append(c.Authorization.Exclude, c.Authorization.Overrides[0].Match)
		}},
		{"wildcard exclusion", func(c *Config) { c.Authorization.Exclude[0].Path = "/*" }},
		{"scoped policy", func(c *Config) {
			p := c.Authorization.Policies["default"]
			p.EntityType = "account"
			c.Authorization.Policies["default"] = p
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			rt, err := druntime.NewRuntime(nil)
			require.NoError(t, err)
			defer rt.Shutdown(context.Background())
			c := authorizationFixture()
			tc.change(&c)
			_, err = c.Build(context.Background(), HandlerInput{Runtime: rt})
			require.Error(t, err)
		})
	}
}

func TestAuthorizationGenerationSnapshot(t *testing.T) {
	c := authorizationFixture()
	h := statusHandler(t, c, time.Now())
	c.Authorization.Policies["default"].Rule.Value = "changed"
	c.Authorization.Exclude[0].Path = "/other"
	require.Equal(t, "*", h.authorization.policies["default"].Rule.Value)
	w := httptest.NewRecorder()
	h.ServeHTTP(w, httptest.NewRequest("GET", "/healthz", nil))
	require.Equal(t, 200, w.Code)
}

func TestAuthorizationTemplateAndDownstreamScope(t *testing.T) {
	f := newConfigFixture(t)
	f.component.Routes[0].Path = "/records/{id}"
	f.component.Settings.Cache = nil
	rt := f.runtime(t)
	defer rt.Shutdown(context.Background())
	calls := 0
	c := authorizationFixture()
	c.Authorization.Overrides = []AuthorizationOverride{{Match: spec.RouteRef{Method: "GET", Path: "/records/{id}"}, Policy: "review"}}
	role := "reader"
	c.AuthorizationProviders["identity"] = func(ctx context.Context, _ *http.Request) (context.Context, authz.Facts, error) {
		calls++
		return ctx, authz.Facts{Subject: "user", Tenant: "tenant", Issuer: "issuer", Roles: []string{role}, ValidUntil: time.Now().Add(time.Minute)}, nil
	}
	downstream := 0
	c.Authorize = func(ctx context.Context, _ *http.Request, _ dexec.ComponentTarget) error {
		downstream++
		require.True(t, AuthorizationFacts(ctx).HasRoleLiteral("reviewer"))
		return authz.ErrDenied // The existing business check remains authoritative.
	}
	h, err := c.Build(context.Background(), HandlerInput{Runtime: rt})
	require.NoError(t, err)
	for _, value := range []string{"reader", "reviewer"} {
		role = value
		w := httptest.NewRecorder()
		h.ServeHTTP(w, httptest.NewRequest("GET", "/records/42?tenant=1", nil))
		require.Equal(t, 403, w.Code)
	}
	require.Equal(t, 2, calls)
	require.Equal(t, 1, downstream)
}
