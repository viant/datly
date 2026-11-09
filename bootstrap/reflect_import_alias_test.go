package bootstrap

import (
	"github.com/stretchr/testify/require"
	imported "github.com/viant/datly/bootstrap/testdata/hyphen-contract"
	"github.com/viant/datly/spec"
	"github.com/viant/xdatly"
	"reflect"
	"testing"
)

type ReflectedHyphenHolder struct {
	Contract xdatly.Component[imported.Input, imported.Output] `component:"ReflectedHyphen,path=/reflected-hyphen,method=POST"`
}

var reflectedHyphenAnchor = reflect.TypeFor[ReflectedHyphenHolder]()

func TestReflectedNonIdentifierPackagePath(t *testing.T) {
	discovered, err := ReflectPackages([]string{reflectedHyphenAnchor.PkgPath()})
	require.NoError(t, err)
	var routes []*RouteSource
	for _, route := range discovered.Components {
		if route.HolderType == "ReflectedHyphenHolder" {
			routes = append(routes, route)
		}
	}
	require.Len(t, routes, 1)
	groups, err := GroupPackageComponentSources(routes, nil)
	require.NoError(t, err)
	require.Equal(t, reflect.TypeFor[imported.Input](), groups[0].InputType.Type)
	require.Equal(t, reflect.TypeFor[imported.Output](), groups[0].OutputType.Type)
}

func TestReflectedImportAliasCollision(t *testing.T) {
	r := &packageComponentResolver{component: &spec.Component{}}
	require.Equal(t, "pkg", r.importAlias("example.com/ordinary/pkg", "pkg"))
	require.Equal(t, "pkg2", r.importAlias("example.com/hyphen-path", "hyphen-path"))
	require.Equal(t, "pkg2", r.importAlias("example.com/hyphen-path", "hyphen-path"))
	require.Equal(t, "pkg3", r.importAlias("example.com/type", "type"))
	require.Equal(t, "valid", r.importAlias("example.com/valid", "valid"))
}
