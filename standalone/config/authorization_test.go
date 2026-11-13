package config_test

import (
	"context"
	"github.com/stretchr/testify/require"
	"github.com/viant/authz"
	"github.com/viant/datly/standalone/config"
	"os"
	"path/filepath"
	"testing"
)

func TestAuthorizationNestedHTTPConfiguration(t *testing.T) {
	raw := `GoBootstrap:
  Packages: [example.com/api]
HTTP:
  Meta:
    StatusURI: /healthz
  Authorization:
    Provider: identity
    DefaultPolicy: authenticated
    Policies:
      authenticated:
        mode: protected
        requiredScopes: [api.read]
        rule:
          kind: subject
          value: "*"
    Overrides:
      - Match:
          Method: GET
          Path: /records/{id}
        Policy: authenticated
    Exclude:
      - Method: GET
        Path: /healthz
`
	file := filepath.Join(t.TempDir(), "config.yaml")
	require.NoError(t, os.WriteFile(file, []byte(raw), 0600))
	c, err := (config.Loader{}).Load(context.Background(), file)
	require.NoError(t, err)
	require.Equal(t, "/healthz", c.Meta.StatusURI)
	require.Equal(t, []string{"api.read"}, c.Authorization.Policies["authenticated"].RequiredScopes)
	require.Equal(t, "GET", c.Authorization.Overrides[0].Match.Method)
	require.Equal(t, "/healthz", c.Authorization.Exclude[0].Path)
	clone, err := c.ResolveConstants()
	require.NoError(t, err)
	require.Equal(t, c.Authorization, clone.Authorization)
	require.NoError(t, os.WriteFile(file, []byte("Meta:\n  StatusURI: /other\n"+raw), 0600))
	_, err = (config.Loader{}).Load(context.Background(), file)
	require.ErrorContains(t, err, "cannot be combined")
}

func TestAuthorizationDocumentedExample(t *testing.T) {
	c, err := (config.Loader{}).Load(context.Background(), "../../doc/examples/http-authorization.yaml")
	require.NoError(t, err)
	require.Equal(t, "standard-access", c.Authorization.DefaultPolicy)
	for _, policy := range c.Authorization.Policies {
		require.NoError(t, authz.ValidatePolicy("execute", policy))
	}
}
