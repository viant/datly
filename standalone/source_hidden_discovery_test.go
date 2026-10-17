package standalone

import (
	"context"
	"net/http/httptest"
	"path/filepath"
	"reflect"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/viant/datly/bootstrap"
	"github.com/viant/datly/bootstrap/connector"
	"github.com/viant/datly/standalone/config"
	"github.com/viant/datly/standalone/testdata/hiddendiscovery"
	"github.com/viant/datly/transcribe"
	"github.com/viant/datly/transcribe/column"
)

// All database ownership and discovery stays behind the Datly component API;
// the fixture creates no tables and never obtains a raw SQL connection.
func TestStandaloneLinkedHiddenComputedDiscovery(t *testing.T) {
	ctx := context.Background()
	cfg := &config.Config{
		BaseDir:     filepath.Join(t.TempDir(), "no-source"),
		Connector:   "main",
		Connectors:  []connector.Config{{Name: "main", Driver: "sqlite3", DSN: filepath.Join(t.TempDir(), "fixture.db")}},
		GoBootstrap: &config.Packages{Packages: []string{hiddendiscovery.LinkedType.PkgPath()}, LinkedOnly: true},
	}
	server, err := New(ctx, Options{Config: cfg})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, server.Shutdown(ctx)) })
	require.NoError(t, server.Reload(ctx, 1))
	call := func(path string) *httptest.ResponseRecorder {
		response := httptest.NewRecorder()
		server.ServeHTTP(response, httptest.NewRequest("GET", path, nil))
		return response
	}
	good := call("/hidden-discovery")
	require.Equal(t, 200, good.Code, good.Body.String())
	require.JSONEq(t, `{"rows":[{"id":1}]}`, good.Body.String())

	// Linked-only runtime consumes the declared Go row contract. Authoring
	// discovery owns validation of SQL results that have no declared field.
	connections, err := connector.Open(ctx, cfg.Connectors, cfg.Connector)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, connections.Close()) })
	reflected, err := bootstrap.ReflectSelectedPackages([]string{hiddendiscovery.LinkedType.PkgPath()}, nil)
	require.NoError(t, err)
	require.Len(t, reflected.Components, 2)
	require.ElementsMatch(t, []string{"Read", "Unknown"}, []string{reflected.Components[0].FieldName, reflected.Components[1].FieldName})
	for _, route := range reflected.Components {
		t.Run(route.FieldName, func(t *testing.T) {
			outputType := reflect.TypeFor[hiddendiscovery.Output]()
			if route.FieldName == "Unknown" {
				outputType = reflect.TypeFor[hiddendiscovery.UnknownOutput]()
			}
			result, err := (&transcribe.PackageCompilation{
				Source:    &transcribe.Source{Scope: hiddendiscovery.LinkedType.PkgPath(), Name: route.FieldName, Connector: "main", ColumnRefiner: column.New(connections)},
				Component: route, InputType: reflect.TypeFor[hiddendiscovery.Input](), OutputType: outputType,
			}).Compile(ctx)
			if route.FieldName == "Unknown" {
				require.ErrorContains(t, err, "unable discover column unowned type")
				return
			}
			require.NoError(t, err)
			require.NotNil(t, result.Component.RootView)
			require.Len(t, result.Component.RootView.Columns, 5)
			found := map[string]bool{}
			for _, discovered := range result.Component.RootView.Columns {
				found[discovered.Source] = true
			}
			for _, name := range []string{"id", "binary_body", "nullable_body", "lease_until_raw", "cleanup_status"} {
				require.True(t, found[name], name)
			}
		})
	}
}
