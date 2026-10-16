package transcribe

import (
	"context"
	"go/ast"
	"os"
	"path/filepath"
	"reflect"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/viant/datly/internal/testharness"
	"github.com/viant/datly/transcribe/column"
	"github.com/viant/datly/transcribe/internal/dependency"
	_ "github.com/viant/datly/transcribe/testdata/linkedimports/auth"
	"github.com/viant/datly/typecatalog"
	"github.com/viant/x"
	xmodule "github.com/viant/x/module"
	"github.com/viant/xunsafe"
)

const linkedImportsModule = "github.com/viant/datly/transcribe/testdata/linkedimports"

func linkedImportsWorkspace(t *testing.T) string {
	t.Helper()
	base := t.TempDir()
	writeSourceFile(t, base, "go.mod", "module "+linkedImportsModule+"\n\ngo 1.25.0\n")
	for _, name := range []string{"auth/output.go", "auth/component.go", "status/status.go"} {
		data, err := os.ReadFile(filepath.Join("testdata/linkedimports", name))
		require.NoError(t, err)
		writeSourceFile(t, base, name, string(data))
	}
	return base
}

func TestDQLImportedLinkedTypesReachColumnDiscovery(t *testing.T) {
	ctx := context.Background()
	base := linkedImportsWorkspace(t)
	writeSourceFile(t, base, "reader/read.dql", `#import('auth', '`+linkedImportsModule+`/auth')
#setting($_ = $route('/read', 'GET'))
#set($_ = $Auth<*auth.Output>(component/GET:/auth))
SELECT id, subject FROM records`)
	db := testharness.NewSQLiteHarness(t)
	require.NoError(t, db.ExecStatements(ctx, "CREATE TABLE records(id INTEGER NOT NULL, subject TEXT NOT NULL)"))
	connections := &discoveryConnections{Connections: column.Connections{"main": db.DB}}
	project, err := (&Discovery{
		BaseDir: base, Include: []string{linkedImportsModule + "/reader"},
		Connector: "main", ColumnRefiner: column.New(connections),
	}).Compile(ctx)
	require.NoError(t, err)
	require.Len(t, project.Components, 1, "imported component holders must not be discovered")
	require.NotEmpty(t, connections.names, "database discovery must actually run")
	compiled := project.Components[0]
	require.Len(t, compiled.Component.RootView.Columns, 2)
	require.NotEmpty(t, compiled.Component.RootView.Columns[1].DatabaseType)
	require.Equal(t, "string", compiled.Component.RootView.Columns[1].Type.Name)
	assertLinkedImportMetadata(t, compiled.Source.Types)
}

func assertLinkedImportMetadata(t *testing.T, catalog *typecatalog.Catalog) {
	t.Helper()
	for _, item := range []struct{ pkg, name string }{{"auth", "Output"}, {"status", "Status"}} {
		path := linkedImportsModule + "/" + item.pkg
		descriptor, found, err := catalog.Resolve(typecatalog.PackageAuthority, path+"."+item.name)
		require.NoError(t, err)
		require.True(t, found)
		require.NotNil(t, descriptor.Type)
		require.Contains(t, xunsafe.PackageTypes(path), descriptor.Type)
		require.Equal(t, path, descriptor.Type.PkgPath())
		require.Equal(t, descriptor.Type, descriptor.SynteticType.ReflectType)
		require.NotNil(t, descriptor.SynteticType.TypeSpec, "source AST must survive linking")
		if item.name == "Output" {
			_, ok := reflect.PointerTo(descriptor.Type).MethodByName("ContractMethod")
			require.True(t, ok, "compiled method set must be retained")
			fields := descriptor.SynteticType.TypeSpec.Type.(*ast.StructType).Fields.List
			require.Equal(t, "`json:\"subject\"`", fields[1].Tag.Value)
		}
	}
}

func TestDQLImportedLinkedTypesIgnoreLegacyManifest(t *testing.T) {
	base := linkedImportsWorkspace(t)
	writeSourceFile(t, base, "auth/.datly-gen.json", `{"version":5,"owner":"Auth","files":["output.go"]}`)
	workspace, err := (xmodule.LocalWorkspace{BaseDir: base}).Resolve(context.Background())
	require.NoError(t, err)
	catalog := typecatalog.NewCatalog()
	discovery := &dependency.Loader{Workspace: workspace, Catalog: catalog}
	for range 2 {
		_, err = discovery.LoadSource(context.Background(), `#import('auth', '`+linkedImportsModule+`/auth')`, "")
		require.NoError(t, err)
		assertLinkedImportMetadata(t, catalog)
	}
	// A legacy file cannot demote an application-owned linked declaration.
	replacement := x.NewType(reflect.TypeOf(struct{ Override bool }{}), x.WithPkgPath(linkedImportsModule+"/auth"), x.WithName("Output"))
	require.Error(t, catalog.Register(typecatalog.TypeOriginPackage, replacement))

}
