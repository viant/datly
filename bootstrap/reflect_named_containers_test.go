package bootstrap_test

import (
	"github.com/stretchr/testify/require"
	"github.com/viant/datly/bootstrap"
	fixture "github.com/viant/datly/bootstrap/testdata/namedcontainers"
	recursive "github.com/viant/datly/bootstrap/testdata/recursivecontainers"
	"github.com/viant/datly/typecatalog"
	"reflect"
	"testing"
)

func TestReflectedPackageRetainsNamedContainerIdentity(t *testing.T) {
	pkg := reflect.TypeFor[fixture.Item]().PkgPath()
	_ = fixture.Anchors
	_ = fixture.Local()
	found, err := bootstrap.ReflectSelectedPackages([]string{pkg}, nil)
	require.NoError(t, err)
	for _, typ := range []reflect.Type{reflect.TypeFor[fixture.Slice](), reflect.TypeFor[fixture.Array](), reflect.TypeFor[fixture.Map](), reflect.TypeFor[fixture.Item]()} {
		got, ok, err := found.Types.ResolveRuntimeType(typecatalog.PackageAuthority, pkg+"."+typ.Name())
		require.NoError(t, err)
		require.True(t, ok, "missing %s", typ)
		require.Equal(t, typ, got)
	}
	_, ok, err := found.Types.ResolveRuntimeType(typecatalog.PackageAuthority, pkg+".local")
	require.NoError(t, err)
	require.False(t, ok)
}

func TestReflectedPackageRecursiveContainerTerminates(t *testing.T) {
	_ = recursive.Anchor
	typ := reflect.TypeFor[recursive.Recursive]()
	found, err := bootstrap.ReflectSelectedPackages([]string{typ.PkgPath()}, nil)
	require.NoError(t, err)
	got, ok, err := found.Types.ResolveRuntimeType(typecatalog.PackageAuthority, typ.PkgPath()+"."+typ.Name())
	require.NoError(t, err)
	require.True(t, ok)
	require.Equal(t, typ, got)
}
