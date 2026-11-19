package gateway

import (
	"context"
	"encoding/json"
	"github.com/viant/datly/repository"
	shapeCompile "github.com/viant/datly/repository/shape/compile"
	shapeLoad "github.com/viant/datly/repository/shape/load"
	"github.com/viant/datly/view"
	"github.com/viant/gmetric"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/viant/authz"
	"github.com/viant/datly/repository/contract"
)

type authDispatchProbe struct {
	t     *testing.T
	calls int
}

func (p *authDispatchProbe) Dispatch(ctx context.Context, path *contract.Path, options ...contract.Option) (interface{}, error) {
	p.calls++
	opts := contract.NewOptions(options...)
	require.Equal(p.t, "/internal/identity", path.URI)
	require.Equal(p.t, "Bearer fixture", opts.Request.Header.Get("Authorization"))
	require.Equal(p.t, "one", opts.Query.Get("tenant"))
	body, err := io.ReadAll(opts.Request.Body)
	require.NoError(p.t, err)
	require.Empty(p.t, body)
	return "identity", nil
}
func TestAuthorizationNativeDispatcherPreservesEndpointBody(t *testing.T) {
	req := httptest.NewRequest("POST", "/records?tenant=one", strings.NewReader("endpoint payload"))
	req.Header.Set("Authorization", "Bearer fixture")
	probe := &authDispatchProbe{t: t}
	invoker := &authorizationComponentInvoker{owner: probe, request: req}
	ctx := context.WithValue(req.Context(), authorizationInvokerKey{}, invoker)
	result, err := AuthorizationComponentInvoker(ctx).Dispatch(ctx, contract.NewPath("GET", "/internal/identity"))
	require.NoError(t, err)
	require.Equal(t, "identity", result)
	require.Equal(t, 1, probe.calls)
	original, err := io.ReadAll(req.Body)
	require.NoError(t, err)
	require.Equal(t, "endpoint payload", string(original))
	require.Nil(t, AuthorizationComponentInvoker(context.Background()))
}

func TestAuthorizationProviderRegistration(t *testing.T) {
	original := &Config{}
	providers := map[string]AuthorizationProvider{"identity": func(ctx context.Context, _ *http.Request) (context.Context, authz.Facts, error) {
		return ctx, authz.Facts{}, nil
	}}
	options, err := newOptions(context.Background(), WithConfig(original), WithAuthorizationProviders(providers))
	require.NoError(t, err)
	delete(providers, "identity")
	require.NotNil(t, options.config.AuthorizationProviders["identity"])
	require.Nil(t, original.AuthorizationProviders)
}

func TestAuthorizationNativeReaderDispatch(t *testing.T) {
	ctx := context.Background()
	repo, err := repository.New(ctx, repository.WithComponentURL(""), repository.WithNoPlugin(), repository.WithRefreshDisabled(true))
	require.NoError(t, err)
	connectors, err := repo.Resources().Lookup(view.ResourceConnectors)
	require.NoError(t, err)
	connectors.Connectors = append(connectors.Connectors, &view.Connector{Connection: view.Connection{DBConfig: view.DBConfig{Name: "identity", Driver: "sqlite3", DSN: ":memory:"}}})
	file := filepath.Join(t.TempDir(), "identity.dql")
	require.NoError(t, os.WriteFile(file, []byte("#setting($_ = $connector('identity'))\n#setting($_ = $route('/internal/identity', 'GET'))\n#define($_ = $Data<?>(output/view).Embed())\nSELECT 'reader' AS role"), 0600))
	component, err := compileBootstrapComponent(ctx, shapeCompile.New(), shapeLoad.New(), repo, file, &DQLBootstrap{}, "")
	require.NoError(t, err)
	repo.Registry().Register(component)
	config := authorizationFixture()
	require.NoError(t, config.Init(ctx))
	config.AuthorizationProviders = map[string]AuthorizationProvider{"identity": func(ctx context.Context, r *http.Request) (context.Context, authz.Facts, error) {
		result, err := AuthorizationComponentInvoker(ctx).Dispatch(ctx, contract.NewPath("GET", "/internal/identity"))
		if err != nil {
			return ctx, authz.Facts{}, err
		}
		raw, err := json.Marshal(result)
		require.NoError(t, err)
		require.Contains(t, string(raw), "reader")
		return ctx, authz.Facts{Subject: "fixture", Tenant: "tenant", Issuer: "fixture", Roles: []string{"reader"}, ValidUntil: time.Now().Add(time.Minute)}, nil
	}}
	router, err := NewRouter(ctx, repo, config, gmetric.New(), http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(200) }), nil)
	require.NoError(t, err)
	response := httptest.NewRecorder()
	router.Handle(response, httptest.NewRequest("GET", config.Meta.ConfigURI, nil))
	require.Equal(t, 200, response.Code, response.Body.String())
}
