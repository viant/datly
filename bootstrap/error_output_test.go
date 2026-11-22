package bootstrap

import (
	"reflect"
	"testing"

	"github.com/stretchr/testify/require"
	fixture "github.com/viant/datly/bootstrap/testdata/expliciterror"
	"github.com/viant/datly/spec"
)

func TestLinkedErrorOutputDeclarations(t *testing.T) {
	pkg := reflect.TypeOf(*fixture.Linked).PkgPath()
	reflected, err := ReflectSelectedPackages([]string{pkg}, nil)
	require.NoError(t, err)
	require.Len(t, reflected.Components, 1)
	groups, err := GroupPackageComponentSources(reflected.Components, nil)
	require.NoError(t, err)
	require.Len(t, groups, 1)
	component, err := groups[0].ResolveDescriptors(nil)
	require.NoError(t, err)
	want := []spec.ErrorOutput{{Type: reflect.TypeFor[fixture.ErrorBody](), Presentation: spec.ErrorOutputLegacyText}}
	require.Equal(t, want, component.ErrorOutputs)

	single, err := reflected.Components[0].Resolve(reflect.TypeFor[fixture.Input](), reflect.TypeFor[fixture.Output]())
	require.NoError(t, err)
	require.Equal(t, want, single.ErrorOutputs)
	clone := component.Clone()
	clone.ErrorOutputs[0].Presentation = spec.ErrorOutputJSON
	require.Equal(t, want, component.ErrorOutputs)
}
