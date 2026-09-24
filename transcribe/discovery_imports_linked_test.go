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
	_ "github.com/viant/datly/transcribe/testdata/linkedimports/auth"
	"github.com/viant/datly/typecatalog"
	"github.com/viant/x"
	xmodule "github.com/viant/x/module"
	smodel "github.com/viant/x/syntetic/model"
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

func TestDQLImportedLinkedTypesKeepManifestOwnership(t *testing.T) {
	base := linkedImportsWorkspace(t)
	writeSourceFile(t, base, "auth/.datly-gen.json", `{"version":5,"owner":"Auth","files":["output.go"]}`)
	workspace, err := (xmodule.LocalWorkspace{BaseDir: base}).Resolve(context.Background())
	require.NoError(t, err)
	catalog := typecatalog.NewCatalog()
	discovery := &dqlPackageDiscovery{workspace: workspace, catalog: catalog}
	for range 2 {
		_, err = discovery.loadSource(context.Background(), `#import('auth', '`+linkedImportsModule+`/auth')`)
		require.NoError(t, err)
		assertLinkedImportMetadata(t, catalog)
	}
	// A generated descriptor must not have been promoted to package authority.
	// A new authored declaration can shadow it without a same-origin conflict.
	replacement := x.NewType(reflect.TypeOf(struct{ Override bool }{}), x.WithPkgPath(linkedImportsModule+"/auth"), x.WithName("Output"))
	require.NoError(t, catalog.Register(typecatalog.TypeOriginPackage, replacement))
	actual, found, err := catalog.ResolveRuntimeType(typecatalog.PackageAuthority, replacement.Key())
	require.NoError(t, err)
	require.True(t, found)
	require.Equal(t, replacement.Type, actual)
}

func firstLocalImportType() reflect.Type {
	type Record struct{ ID int }
	return reflect.TypeOf(Record{})
}

func secondLocalImportType() reflect.Type {
	type Record struct{ ID int }
	return reflect.TypeOf(Record{})
}

func TestLinkSourcePackageIdentityValidation(t *testing.T) {
	first, second := firstLocalImportType(), secondLocalImportType()
	require.NotEqual(t, first, second)
	for _, test := range []struct {
		name     string
		linked   []reflect.Type
		explicit reflect.Type
		existing reflect.Type
		want     reflect.Type
		err      string
	}{
		{name: "linked", linked: []reflect.Type{first}, want: first},
		{name: "duplicate same identity", linked: []reflect.Type{first, first}, want: first},
		{name: "ambiguous local names", linked: []reflect.Type{first, second}, err: "ambiguous linked compiled identities"},
		{name: "registry precedence", linked: []reflect.Type{first, second}, explicit: second, want: second},
		{name: "existing identity", existing: first, want: first},
		{name: "existing conflict", linked: []reflect.Type{second}, existing: first, err: "different compiled identity"},
		{name: "explicit conflict", explicit: second, existing: first, err: "different compiled identity"},
		{name: "source only"},
	} {
		t.Run(test.name, func(t *testing.T) {
			catalog := typecatalog.NewCatalog()
			registry := x.NewRegistry()
			if test.explicit != nil {
				registry.Register(x.NewType(test.explicit))
			}
			if test.existing != nil {
				require.NoError(t, catalog.Register(typecatalog.TypeOriginPackage, x.NewType(test.existing)))
			}
			declared := &smodel.Type{Name: first.Name()}
			pkg := &smodel.Package{PkgPath: first.PkgPath(), Types: []*smodel.Type{declared}}
			err := (&dqlPackageDiscovery{catalog: catalog, registry: registry}).linkSourcePackage(pkg, test.linked)
			if test.err != "" {
				require.ErrorContains(t, err, test.err)
				require.Nil(t, declared.ReflectType)
			} else {
				require.NoError(t, err)
				require.Equal(t, test.want, declared.ReflectType)
			}
			if test.existing != nil {
				actual, _, err := catalog.ResolveRuntimeType(typecatalog.PackageAuthority, first.PkgPath()+"."+first.Name())
				require.NoError(t, err)
				require.Equal(t, test.existing, actual, "linking must not mutate the active catalog")
			}
		})
	}
}
