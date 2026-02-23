package gateway

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/viant/datly/repository"
	"github.com/viant/datly/repository/contract"
	"github.com/viant/datly/view"
)

func TestConfigValidate_AllowsEmptyRouteURLWithDQLBootstrap(t *testing.T) {
	cfg := &Config{
		ExposableConfig: ExposableConfig{
			DQLBootstrap: &DQLBootstrap{
				Sources: []string{"./testdata/*.dql"},
			},
		},
	}
	require.NoError(t, cfg.Validate())
}

func TestConfigValidate_FailsWithoutRouteAndBootstrap(t *testing.T) {
	cfg := &Config{}
	require.ErrorContains(t, cfg.Validate(), "RouteURL was empty")
}

func TestConfigValidate_FailsForEmptyBootstrapSources(t *testing.T) {
	cfg := &Config{
		ExposableConfig: ExposableConfig{
			DQLBootstrap: &DQLBootstrap{},
		},
	}
	require.ErrorContains(t, cfg.Validate(), "DQLBootstrap.Sources was empty")
}

func TestDiscoverDQLBootstrapSources(t *testing.T) {
	root := t.TempDir()
	require.NoError(t, os.MkdirAll(filepath.Join(root, "sql", "nested"), 0o755))
	require.NoError(t, os.WriteFile(filepath.Join(root, "sql", "a.dql"), []byte("SELECT 1"), 0o644))
	require.NoError(t, os.WriteFile(filepath.Join(root, "sql", "nested", "b.sql"), []byte("SELECT 2"), 0o644))
	require.NoError(t, os.WriteFile(filepath.Join(root, "sql", "nested", "skip.dql"), []byte("SELECT 3"), 0o644))

	sources, err := discoverDQLBootstrapSources(
		[]string{filepath.Join(root, "sql", "**", "*")},
		[]string{filepath.Join(root, "sql", "**", "skip.dql")},
	)
	require.NoError(t, err)
	require.Len(t, sources, 2)
	assert.Contains(t, sources, filepath.Join(root, "sql", "a.dql"))
	assert.Contains(t, sources, filepath.Join(root, "sql", "nested", "b.sql"))
}

func TestResolvePathSettings(t *testing.T) {
	method, uri := resolvePathSettings("/tmp/orders/get.dql", `/* {"Method":"POST","URI":"/v1/api/orders"} */ SELECT 1`, "/v1/api")
	assert.Equal(t, "POST", method)
	assert.Equal(t, "/v1/api/orders", uri)

	method, uri = resolvePathSettings("/tmp/orders/get.dql", `SELECT 1`, "/v1/api")
	assert.Equal(t, "GET", method)
	assert.Equal(t, "/v1/api/get", uri)
}

func TestDQLBootstrapEffectivePrecedence(t *testing.T) {
	assert.Equal(t, DQLBootstrapPrecedenceRoutesWins, (&DQLBootstrap{}).EffectivePrecedence())
	assert.Equal(t, DQLBootstrapPrecedenceDQLWins, (&DQLBootstrap{Precedence: "dql_wins"}).EffectivePrecedence())
	assert.Equal(t, DQLBootstrapPrecedenceRoutesWins, (&DQLBootstrap{Precedence: "unknown"}).EffectivePrecedence())
}

func TestApplyDQLBootstrap_Precedence(t *testing.T) {
	ctx := context.Background()
	repo, err := repository.New(ctx, repository.WithComponentURL(""), repository.WithNoPlugin())
	require.NoError(t, err)

	route := contract.Path{Method: "GET", URI: "/v1/api/test"}
	repo.Register(&repository.Component{Path: route})
	connectors, err := repo.Resources().Lookup(view.ResourceConnectors)
	require.NoError(t, err)
	connectors.Connectors = append(connectors.Connectors, &view.Connector{
		Connection: view.Connection{
			DBConfig: view.DBConfig{
				Name:   "test_conn",
				Driver: "sqlite3",
				DSN:    "sqlite:./test.db",
			},
		},
	})

	root := t.TempDir()
	source := filepath.Join(root, "test.dql")
	require.NoError(t, os.WriteFile(source, []byte(`/* {"Method":"GET","URI":"/v1/api/test","Connector":"test_conn"} */ SELECT 1 AS id`), 0o644))
	srv := &Service{Config: &Config{ExposableConfig: ExposableConfig{APIPrefix: "/v1/api"}}}

	routesWins := &DQLBootstrap{
		Sources:    []string{source},
		Precedence: DQLBootstrapPrecedenceRoutesWins,
	}
	require.NoError(t, srv.applyDQLBootstrap(ctx, repo, routesWins))
	provider, err := repo.Registry().LookupProvider(ctx, &route)
	require.NoError(t, err)
	require.NotNil(t, provider)
	component, err := provider.Component(ctx)
	require.NoError(t, err)
	assert.Nil(t, component.View)

	dqlWins := &DQLBootstrap{
		Sources:    []string{source},
		Precedence: DQLBootstrapPrecedenceDQLWins,
	}
	require.NoError(t, srv.applyDQLBootstrap(ctx, repo, dqlWins))
	provider, err = repo.Registry().LookupProvider(ctx, &route)
	require.NoError(t, err)
	require.NotNil(t, provider)
	component, err = provider.Component(ctx)
	require.NoError(t, err)
	require.NotNil(t, component.View)
	assert.Equal(t, "test", component.View.Name)
}
