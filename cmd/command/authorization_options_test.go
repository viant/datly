package command

import (
	"context"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/viant/authz"
	"github.com/viant/datly/cmd/options"
	"github.com/viant/datly/gateway"
)

func TestRunPassesTrustedAuthorizationProviders(t *testing.T) {
	gateway.ResetSingleton()
	defer gateway.ResetSingleton()
	root := t.TempDir()
	routes := filepath.Join(root, "routes")
	require.NoError(t, os.Mkdir(routes, 0700))
	config := filepath.Join(root, "config.json")
	require.NoError(t, os.WriteFile(config, []byte(`{"RouteURL":`+"\""+routes+"\""+`,"Meta":{"StatusURI":"/healthz"},"Authorization":{"Provider":"fixture","DefaultPolicy":"read","Policies":{"read":{"mode":"protected","rule":{"kind":"subject","value":"*"}}},"Exclude":[{"Method":"GET","Path":"/healthz"}]}}`), 0600))
	calls := 0
	svc := New(gateway.WithAuthorizationProviders(map[string]gateway.AuthorizationProvider{"fixture": func(ctx context.Context, _ *http.Request) (context.Context, authz.Facts, error) {
		calls++
		return ctx, authz.Facts{}, gateway.ErrUnauthenticated
	}}))
	server, err := svc.run(context.Background(), &options.Run{ConfigURL: config})
	require.NoError(t, err)
	defer server.Service.Close()
	for _, tc := range []struct {
		path   string
		status int
	}{{"/healthz", 200}, {"/private", 401}} {
		w := httptest.NewRecorder()
		server.Service.ServeHTTP(w, httptest.NewRequest("GET", tc.path, nil))
		require.Equal(t, tc.status, w.Code, w.Body.String())
	}
	require.Equal(t, 1, calls)
}
