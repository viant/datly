package http

import (
	"context"
	"github.com/stretchr/testify/require"
	"github.com/viant/authz"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

func TestAuthorizationNegationHTTP(t *testing.T) {
	for _, tc := range []struct {
		name  string
		roles []string
		want  int
	}{
		{"editor", []string{"editor"}, 404},
		{"suspended editor", []string{"editor", "suspended"}, 403},
		{"no editor", nil, 403},
	} {
		t.Run(tc.name, func(t *testing.T) {
			c := authorizationFixture()
			c.Authorization.Policies["default"] = authz.Policy{Mode: "protected", Rule: &authz.Rule{Kind: "all", Rules: []authz.Rule{{Kind: "role", Value: "editor"}, {Kind: "not", Rules: []authz.Rule{{Kind: "role", Value: "suspended"}}}}}}
			c.AuthorizationProviders["identity"] = func(ctx context.Context, _ *http.Request) (context.Context, authz.Facts, error) {
				return ctx, authz.Facts{Subject: "user", Tenant: "tenant", Issuer: "issuer", Roles: tc.roles, ValidUntil: time.Now().Add(time.Minute)}, nil
			}
			h := statusHandler(t, c, time.Now())
			w := httptest.NewRecorder()
			h.ServeHTTP(w, httptest.NewRequest("GET", "/missing", nil))
			require.Equal(t, tc.want, w.Code)
		})
	}
}
