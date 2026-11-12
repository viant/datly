package gateway

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/viant/afs"
	"github.com/viant/authz"
)

func TestAuthorizationConfigLoadAndSnapshot(t *testing.T) {
	path := filepath.Join(t.TempDir(), "config.yaml")
	require.NoError(t, os.WriteFile(path, []byte(`RouteURL: mem://routes
Authorization:
  Provider: identity
  DefaultPolicy: read
  Policies:
    read:
      mode: protected
      rule:
        kind: all
        rules:
          - {kind: role, value: reader}
          - kind: not
            rules:
              - {kind: role, value: suspended}
  Exclude:
    - {Method: GET, Path: /healthz}
`), 0600))
	config, err := NewConfigFromURL(context.Background(), afs.New(), path)
	require.NoError(t, err)
	config.AuthorizationProviders = map[string]AuthorizationProvider{"identity": func(ctx context.Context, _ *http.Request) (context.Context, authz.Facts, error) {
		return ctx, authz.Facts{}, ErrUnauthenticated
	}}
	gate, err := config.authorizationGate()
	require.NoError(t, err)
	config.Authorization.Policies["read"].Rule.Rules[0].Value = "changed"
	require.Equal(t, "reader", gate.policies["read"].Rule.Rules[0].Value)
	require.True(t, gate.exclude[AuthorizationMatch{Method: "GET", Path: "/healthz"}])
}

func TestAuthorizationInvalidConfiguration(t *testing.T) {
	for _, tc := range []struct {
		name   string
		change func(*Config)
	}{
		{"provider missing", func(c *Config) { c.AuthorizationProviders = nil }},
		{"default missing", func(c *Config) { c.Authorization.DefaultPolicy = "" }},
		{"unknown override", func(c *Config) { c.Authorization.Overrides[0].Policy = "missing" }},
		{"conflict", func(c *Config) {
			c.Authorization.Exclude = append(c.Authorization.Exclude, c.Authorization.Overrides[0].Match)
		}},
		{"wildcard exclusion", func(c *Config) { c.Authorization.Exclude[0].Path = "/*" }},
		{"invalid negation", func(c *Config) {
			c.Authorization.Policies["reader"] = authz.Policy{Mode: "protected", Rule: &authz.Rule{Kind: "not"}}
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			c := authorizationFixture()
			c.AuthorizationProviders = map[string]AuthorizationProvider{"identity": func(ctx context.Context, _ *http.Request) (context.Context, authz.Facts, error) {
				return ctx, authz.Facts{}, nil
			}}
			tc.change(c)
			_, err := c.authorizationGate()
			require.Error(t, err)
		})
	}
}

func TestAuthorizationDisabledAndFailures(t *testing.T) {
	c := authorizationFixture()
	c.Authorization = nil
	r := authorizationRouter(t, c, new(atomic.Int32))
	w := httptest.NewRecorder()
	r.Handle(w, httptest.NewRequest("GET", "/healthz", nil))
	require.Equal(t, 200, w.Code)
	for _, tc := range []struct {
		name    string
		failure error
		code    int
	}{{"cancel", context.Canceled, 408}, {"deadline", context.DeadlineExceeded, 504}, {"dependency", errors.New("private message"), 503}} {
		t.Run(tc.name, func(t *testing.T) {
			c := authorizationFixture()
			c.AuthorizationProviders = map[string]AuthorizationProvider{"identity": func(ctx context.Context, _ *http.Request) (context.Context, authz.Facts, error) {
				return ctx, authz.Facts{}, tc.failure
			}}
			r := authorizationRouter(t, c, new(atomic.Int32))
			request := httptest.NewRequest("GET", "/records", nil)
			w := httptest.NewRecorder()
			code, err := r.handleRoute(w, request)
			require.Equal(t, tc.code, code)
			require.ErrorIs(t, err, tc.failure)
			r.handleErrorCode(w, code, err)
			require.Equal(t, tc.code, w.Code)
			require.NotContains(t, w.Body.String(), "private message")
		})
	}
}

func TestAuthorizationDocumentedExample(t *testing.T) {
	c, err := NewConfigFromURL(context.Background(), afs.New(), "../doc/examples/http-authorization.yaml")
	require.NoError(t, err)
	c.AuthorizationProviders = map[string]AuthorizationProvider{"userinfo": func(ctx context.Context, _ *http.Request) (context.Context, authz.Facts, error) {
		return ctx, authz.Facts{}, ErrUnauthenticated
	}}
	_, err = c.authorizationGate()
	require.NoError(t, err)
}
