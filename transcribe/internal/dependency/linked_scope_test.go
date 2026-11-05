package dependency

import (
	"reflect"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/viant/datly/typecatalog"
)

type LinkedScopePublic struct{ ID int }

func linkedScopeFirst() reflect.Type {
	type connectionStater struct{ First int }
	return reflect.TypeOf(&connectionStater{}).Elem()
}

func linkedScopeSecond() reflect.Type {
	type connectionStater struct{ Second string }
	return reflect.TypeOf(&connectionStater{}).Elem()
}

func TestLinkedImportsIgnorePrivateScopedNames(t *testing.T) {
	first, second := linkedScopeFirst(), linkedScopeSecond()
	require.Equal(t, first.Name(), second.Name())
	require.Equal(t, first.PkgPath(), second.PkgPath())
	require.NotEqual(t, first, second)
	public := reflect.TypeOf(&LinkedScopePublic{}).Elem()
	catalog := typecatalog.NewCatalog()
	loader := &Loader{Catalog: catalog}
	require.NoError(t, loader.registerLinkedPackageTypes(public.PkgPath()))
	got, found, err := catalog.ResolveRuntimeType(typecatalog.PackageAuthority, public.PkgPath()+"."+public.Name())
	require.NoError(t, err)
	require.True(t, found)
	require.Equal(t, public, got)
	_, found, err = catalog.ResolveRuntimeType(typecatalog.PackageAuthority, first.PkgPath()+"."+first.Name())
	require.NoError(t, err)
	require.False(t, found, "ambiguous local identities must not be chosen arbitrarily")
}
