package transcribe

import (
	"context"
	"embed"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"testing/fstest"

	"github.com/stretchr/testify/require"
	"github.com/viant/bindly/resource"
	"github.com/viant/datly/bootstrap"
	"github.com/viant/datly/internal/testharness"
	"github.com/viant/datly/transcribe/column"
	contracts "github.com/viant/datly/transcribe/testdata/inputdefaults/contracts"
	fixture "github.com/viant/datly/transcribe/testdata/inputdefaults/registration"
)

const inputDefaultModule = "github.com/viant/datly/transcribe/testdata/inputdefaults"

func inputDefaultWorkspace(t *testing.T) string {
	t.Helper()
	base := t.TempDir()
	writeSourceFile(t, base, "go.mod", "module "+inputDefaultModule+"\n\ngo 1.25.0\n")
	for _, rel := range []string{"contracts/contracts.go", "registration/registration.go"} {
		body, err := os.ReadFile(filepath.Join("testdata/inputdefaults", rel))
		require.NoError(t, err)
		writeSourceFile(t, base, rel, string(body))
	}
	return base
}
func TestDiscoveryImportedInputDefaultsReachColumnDiscovery(t *testing.T) {
	base := inputDefaultWorkspace(t)
	db := testharness.NewSQLiteHarness(t)
	require.NoError(t, db.ExecStatements(context.Background(), "CREATE TABLE component_a(id INTEGER)", "CREATE TABLE component_b(id INTEGER)"))
	sdk, err := filepath.Abs("..")
	require.NoError(t, err)
	connections := &discoveryConnections{Connections: column.Connections{"main": db.DB}}
	project, err := (&Discovery{BaseDir: base, ModuleDirs: []string{sdk}, Include: []string{inputDefaultModule + "/registration"}, Connector: "main", ColumnRefiner: column.New(connections), Holders: []any{fixture.ComponentA{}, fixture.ComponentB{}}, RequireLinked: true}).Compile(t.Context())
	require.NoError(t, err)
	require.Len(t, project.Components, 2)
	require.NotEmpty(t, connections.names)
	for _, result := range project.Components {
		want := "SELECT id FROM component_a"
		other := "component_b"
		if result.Component.Key.Name == "ReadB" {
			want = "SELECT id FROM component_b"
			other = "component_a"
		}
		got, err := result.Source.Resources.ReadFile("query.sql")
		require.NoError(t, err)
		require.Equal(t, want, strings.TrimSpace(string(got)))
		require.NotNil(t, result.Source.LinkedInputType)
		require.Equal(t, inputDefaultModule+"/contracts", result.Source.LinkedInputType.PkgPath())
		require.Len(t, result.Component.RootView.Columns, 1)
		require.NotEmpty(t, result.Component.RootView.Columns[0].DatabaseType)
		require.Len(t, result.Component.Views, 1)
		require.Len(t, result.Component.Views[0].Columns, 1)
		require.NotEmpty(t, result.Component.Views[0].Columns[0].DatabaseType)
		require.NotContains(t, result.Component.RootView.Source.SQL, other)
	}
	if _, ok := project.Resources.Lookup(""); ok {
		t.Fatal("input default mutated global discovery resources")
	}
}

func TestPackageInputDefaultResourcesScopeAndNamedAuthority(t *testing.T) {
	global := resource.New()
	require.NoError(t, global.Register("named", fstest.MapFS{"query.sql": {Data: []byte("named query")}, "missing.sql": {Data: []byte("named-only")}}))
	stores := make([]*resource.Store, 0, 2)
	for _, test := range []struct {
		name string
		typ  reflect.Type
		want string
	}{{"value", reflect.TypeFor[contracts.InputA](), "SELECT id FROM component_a"}, {"pointer", reflect.TypeFor[*contracts.InputB](), "SELECT id FROM component_b"}} {
		t.Run(test.name, func(t *testing.T) {
			scoped, err := packageInputDefaultResources(global, test.typ, inputDefaultModule+"/registration")
			require.NoError(t, err)
			require.NotSame(t, global, scoped)
			stores = append(stores, scoped)
			body, err := scoped.ReadFile("query.sql")
			require.NoError(t, err)
			require.Equal(t, test.want, strings.TrimSpace(string(body)))
			body, err = scoped.ReadFile("named:query.sql")
			require.NoError(t, err)
			require.Equal(t, "named query", string(body))
			_, err = scoped.ReadFile("missing.sql")
			require.Error(t, err, "missing selected asset must not fall back to source or named default")
			_, err = scoped.ReadFile("unknown:query.sql")
			require.Error(t, err, "qualified miss must not fall back to input")
		})
	}
	first, err := stores[0].ReadFile("query.sql")
	require.NoError(t, err)
	require.Equal(t, "SELECT id FROM component_a", strings.TrimSpace(string(first)), "constructing B must not replace A's default")
	_, exists := global.Lookup("")
	require.False(t, exists, "input defaults must not mutate global discovery resources")
	require.NoError(t, global.Register("later", fstest.MapFS{"query.sql": {Data: []byte("later named query")}}))
	for _, scoped := range stores {
		body, err := scoped.ReadFile("later:query.sql")
		require.NoError(t, err)
		require.Equal(t, "later named query", string(body))
	}
	scoped, err := packageInputDefaultResources(nil, reflect.TypeFor[contracts.InputA](), inputDefaultModule+"/registration")
	require.NoError(t, err)
	body, err := scoped.ReadFile("query.sql")
	require.NoError(t, err)
	require.Equal(t, "SELECT id FROM component_a", strings.TrimSpace(string(body)))
	authored := resource.New()
	require.NoError(t, authored.Register("", fstest.MapFS{"query.sql": {Data: []byte("SELECT id FROM current_source")}}))
	scoped, err = packageInputDefaultResources(authored, reflect.TypeFor[contracts.InputA](), inputDefaultModule+"/contracts")
	require.NoError(t, err)
	require.Same(t, authored, scoped)
	body, err = scoped.ReadFile("query.sql")
	require.NoError(t, err)
	require.Equal(t, "SELECT id FROM current_source", string(body))
}

type nilPackageInputEmbedder struct{}

func (nilPackageInputEmbedder) EmbedFS() *embed.FS { return nil }
func TestPackageInputDefaultResourcesNoCapabilityKeepsStore(t *testing.T) {
	global := resource.New()
	for _, typ := range []reflect.Type{nil, reflect.TypeFor[int](), reflect.TypeFor[struct{}](), reflect.TypeFor[*struct{}](), reflect.TypeFor[nilPackageInputEmbedder]()} {
		scoped, err := packageInputDefaultResources(global, typ, inputDefaultModule+"/registration")
		require.NoError(t, err)
		require.Same(t, global, scoped)
	}
	if _, ok := global.Lookup(""); ok {
		t.Fatal("no capability must not create a default")
	}
}

func TestPackageCompilationDQLKeepsSourceDefaultBeforeColumns(t *testing.T) {
	ctx := t.Context()
	db := testharness.NewSQLiteHarness(t)
	require.NoError(t, db.ExecStatements(ctx, "CREATE TABLE dql_only(id INTEGER)"))
	reflected, err := bootstrap.ReflectPackages([]string{inputDefaultModule + "/registration"})
	require.NoError(t, err)
	var route *bootstrap.RouteSource
	for _, candidate := range reflected.Components {
		if candidate.HolderType == "ComponentA" {
			route = candidate
		}
	}
	require.NotNil(t, route)
	global := resource.New()
	sourceStore, err := global.WithDefault(fstest.MapFS{"query.sql": {Data: []byte("SELECT id FROM dql_only")}})
	require.NoError(t, err)
	source := &Source{Scope: route.PackagePath, Name: "ReadA", Resources: sourceStore, Connector: "main", ColumnRefiner: column.New(column.Connections{"main": db.DB}), Text: "#setting($_ = $route('/read-a','GET'))\nSELECT id FROM dql_only"}
	compiled, err := (&PackageCompilation{Source: source, Component: route, InputType: route.LinkedInputType, OutputType: route.LinkedOutputType, Types: reflected.Types}).Compile(ctx)
	require.NoError(t, err, "InputA's embedded component_a SQL must not replace authored DQL defaults")
	require.Same(t, sourceStore, compiled.Source.Resources)
	require.Same(t, sourceStore, source.Resources)
	body, err := compiled.Source.Resources.ReadFile("query.sql")
	require.NoError(t, err)
	require.Equal(t, "SELECT id FROM dql_only", string(body))
	require.Len(t, compiled.Component.RootView.Columns, 1)
	require.NotEmpty(t, compiled.Component.RootView.Columns[0].DatabaseType)
	require.Len(t, compiled.Component.Views, 1)
	require.NotEmpty(t, compiled.Component.Views[0].Columns[0].DatabaseType)
}
