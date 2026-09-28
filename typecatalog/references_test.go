package typecatalog

import (
	"github.com/stretchr/testify/require"
	"github.com/viant/x"
	"reflect"
	"testing"
)

func TestImportReferencesPreservesOwnershipAndAtomicity(t *testing.T) {
	source, dest := NewCatalog(), NewCatalog()
	first := x.NewType(reflect.TypeFor[int](), x.WithPkgPath("example.com/codecs"), x.WithName("First"))
	second := x.NewType(reflect.TypeFor[string](), x.WithPkgPath("example.com/codecs"), x.WithName("Second"))
	require.NoError(t, source.Register(TypeOriginGenerated, first))
	require.NoError(t, source.Register(TypeOriginPackage, second))
	require.NoError(t, dest.ImportReferences(source, []string{first.Key()}))
	dest.mu.RLock()
	require.Equal(t, TypeOriginGenerated, dest.items[first.Key()][0].Origin)
	dest.mu.RUnlock()
	_, found, err := dest.Resolve(PackageAuthority, second.Key())
	require.NoError(t, err)
	require.False(t, found)
	conflict := x.NewType(reflect.TypeFor[bool](), x.WithPkgPath("example.com/codecs"), x.WithName("First"))
	bad := NewCatalog()
	require.NoError(t, bad.Register(TypeOriginGenerated, conflict))
	require.NoError(t, bad.Register(TypeOriginPackage, second))
	require.Error(t, dest.ImportReferences(bad, []string{second.Key(), first.Key()}))
	_, found, err = dest.Resolve(PackageAuthority, second.Key())
	require.NoError(t, err)
	require.False(t, found, "conflict cannot partially publish references")
}
