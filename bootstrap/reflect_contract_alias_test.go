package bootstrap

import (
	"github.com/stretchr/testify/require"
	"github.com/viant/xdatly"
	"reflect"
	"testing"
)

type ReflectedAliasOutput = ReflectedPackageOutput
type ReflectedAliasHolder struct {
	Contract xdatly.Component[ReflectedPackageInput, ReflectedAliasOutput] `component:"ReflectedAlias,path=/reflected-alias,method=POST"`
}

var reflectedAliasAnchor = reflect.TypeFor[ReflectedAliasHolder]()

func TestReflectedAliasContractUsesRuntimeIdentity(t *testing.T) {
	discovered, err := ReflectPackages([]string{reflectedAliasAnchor.PkgPath()})
	require.NoError(t, err)
	var selected []*RouteSource
	for _, route := range discovered.Components {
		if route.HolderType == "ReflectedAliasHolder" {
			selected = append(selected, route)
		}
	}
	require.Len(t, selected, 1)
	groups, err := GroupPackageComponentSources(selected, nil)
	require.NoError(t, err)
	require.Len(t, groups, 1)
	require.Equal(t, reflect.TypeFor[ReflectedPackageOutput](), groups[0].OutputType.Type)
	require.NoError(t, selected[0].ValidateContractTypes(reflect.TypeFor[ReflectedPackageInput](), reflect.TypeFor[ReflectedAliasOutput]()))
}
