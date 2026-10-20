package generate

import (
	"reflect"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/viant/datly/spec"
	"github.com/viant/datly/typecatalog"
	"github.com/viant/x"
)

func TestColumnDiscoveryDefersOnlyOutputDTOConcretization(t *testing.T) {
	const pkg = "example.com/app/api"
	component := &spec.Component{Name: "Read", TypeContext: &spec.TypeContext{PackagePath: pkg}, Routes: []*spec.Route{{Method: "GET", Path: "/read"}}, Parameters: []*spec.Parameter{
		{Name: "Id", TypeExpr: "int", Source: spec.BindSource{Kind: "query", Name: "id"}},
		{Name: "Warnings", TypeExpr: "[]*" + pkg + ".Warning", Source: spec.BindSource{Kind: "output", Name: "warnings"}},
	}, RootView: &spec.View{Name: "records", TypeName: "Record", Columns: []*spec.Column{{Name: "id", Type: spec.TypeRef{Name: "int"}}}}}
	catalog := typecatalog.NewCatalog()
	resolver, err := typecatalog.NewResolver(catalog, typecatalog.TranscribeAuthority, &typecatalog.ResolutionContext{PackagePath: pkg})
	require.NoError(t, err)
	generator := New(Input{Component: component, TargetPackage: pkg, TypeResolver: resolver})
	input, err := generator.ColumnDiscoveryInputType()
	require.NoError(t, err)
	require.Equal(t, reflect.Struct, input.Kind())
	require.False(t, generator.input.columnDiscovery)
	_, err = generator.Plan()
	require.ErrorContains(t, err, "package type was not found")
	_, err = generator.RuntimeOutputType()
	require.ErrorContains(t, err, "package type was not found")
	require.NoError(t, catalog.Register(typecatalog.TypeOriginPackage, x.NewType(reflect.TypeOf(struct{ Message string }{}), x.WithPkgPath(pkg), x.WithName("Warning"))))
	resolver, err = typecatalog.NewResolver(catalog, typecatalog.TranscribeAuthority, &typecatalog.ResolutionContext{PackagePath: pkg})
	require.NoError(t, err)
	generator = New(Input{Component: component, TargetPackage: pkg, TypeResolver: resolver})
	_, err = generator.Plan()
	require.NoError(t, err)
	_, err = generator.RuntimeOutputType()
	require.NoError(t, err)
}
