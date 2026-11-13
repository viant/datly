package standalone

import (
	"context"
	"github.com/stretchr/testify/require"
	"github.com/viant/authz"
	gateway "github.com/viant/datly/gateway/http"
	"github.com/viant/datly/spec"
	"github.com/viant/datly/standalone/config"
	fixture "github.com/viant/datly/standalone/testdata/app"
	"github.com/viant/datly/standalone/testdata/app/records"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

func TestStandaloneAuthorizationProviderAndReload(t *testing.T) {
	ctx := context.Background()
	f := fixture.New(t)
	cfg, err := (config.Loader{}).Load(ctx, f.Config)
	require.NoError(t, err)
	cfg.Meta.StatusURI = "/healthz"
	cfg.Authorization = &gateway.AuthorizationConfig{Provider: "identity", DefaultPolicy: "read", Policies: map[string]authz.Policy{"read": {Mode: "protected", Rule: &authz.Rule{Kind: "role", Value: "reader"}}}, Exclude: []spec.RouteRef{{Method: "GET", Path: "/healthz"}}}
	exports, err := records.Exports()
	require.NoError(t, err)
	calls := 0
	s, err := New(ctx, Options{Config: cfg, Registry: exports, AuthorizationProviders: map[string]gateway.AuthorizationProvider{"identity": func(ctx context.Context, r *http.Request) (context.Context, authz.Facts, error) {
		calls++
		if r.Header.Get("Authorization") == "" {
			return ctx, authz.Facts{}, gateway.ErrUnauthenticated
		}
		return ctx, authz.Facts{Subject: "fixture", Tenant: "tenant", Issuer: "issuer", Roles: []string{"reader"}, ValidUntil: time.Now().Add(time.Minute)}, nil
	}}})
	require.NoError(t, err)
	defer s.Shutdown(ctx)
	require.NoError(t, s.Reload(ctx, 1))
	for _, tc := range []struct {
		path, credential string
		status           int
	}{{"/healthz", "", 200}, {"/records/1", "", 401}, {"/records/1", "fixture", 200}} {
		req := httptest.NewRequest("GET", tc.path, nil)
		req.Header.Set("Authorization", tc.credential)
		w := httptest.NewRecorder()
		s.ServeHTTP(w, req)
		require.Equal(t, tc.status, w.Code, w.Body.String())
	}
	require.Equal(t, 2, calls)
	// A failed policy generation must leave the previous handler in service.
	s.source.http.Authorization.DefaultPolicy = "missing"
	require.Error(t, s.Reload(ctx, 2))
	w := httptest.NewRecorder()
	s.ServeHTTP(w, httptest.NewRequest("GET", "/records/1", nil))
	require.Equal(t, 401, w.Code)
}
