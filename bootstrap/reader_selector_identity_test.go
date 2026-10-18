package bootstrap_test

import (
	"reflect"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/viant/datly/bootstrap"
	fixture "github.com/viant/datly/bootstrap/testdata/selectorprefixed"
	"github.com/viant/datly/internal/readergraph"
	"github.com/viant/x"
)

func TestCheckedReaderCompanionPrefixedStorageIdentity(t *testing.T) {
	found, err := bootstrap.ReflectSelectedPackages([]string{reflect.TypeFor[fixture.Component]().PkgPath()}, nil)
	require.NoError(t, err)
	require.Len(t, found.Components, 1)
	component, err := found.Components[0].Resolve(reflect.TypeFor[fixture.Input](), reflect.TypeFor[fixture.Output]())
	require.NoError(t, err)
	require.NoError(t, bootstrap.CompileOutputColumns(component, reflect.TypeFor[fixture.Output]()))
	component, err = readergraph.Admit(component, x.NewType(reflect.TypeFor[fixture.Input]()), x.NewType(reflect.TypeFor[fixture.Output]()), nil)
	require.NoError(t, err)
	component.ReaderGraph = component.ReaderGraph.WithSelector(component.SelectorCompanion)
	_, err = (bootstrap.ContractResolver{Component: component, InputType: x.NewType(reflect.TypeFor[fixture.Input]()), OutputType: x.NewType(reflect.TypeFor[fixture.Output]())}).Resolve()
	require.NoError(t, err)
}
