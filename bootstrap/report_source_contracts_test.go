package bootstrap

import (
	"github.com/stretchr/testify/require"
	fixture "github.com/viant/datly/bootstrap/testdata/selectorcompanion"
	"github.com/viant/datly/spec"
	"reflect"
	"testing"
)

func TestReportSourceCanonicalSelectorContracts(t *testing.T) {
	found, err := ReflectSelectedPackages([]string{reflect.TypeFor[fixture.Component]().PkgPath()}, nil)
	require.NoError(t, err)
	component, err := found.Components[0].Resolve(reflect.TypeFor[fixture.Input](), reflect.TypeFor[fixture.Output]())
	require.NoError(t, err)
	resolved, err := ResolveReportSourceWithContracts(component, reflect.TypeFor[fixture.Input](), reflect.TypeFor[fixture.Output](), nil)
	require.NoError(t, err)
	assertCompanionMetadata(t, resolved)
	require.NotSame(t, component, resolved)
	resolved.RootView.Source.SQL = "changed"
	require.NotEqual(t, "changed", component.RootView.Source.SQL)
	_, err = ResolveReportSource(component, reflect.TypeFor[fixture.Output](), nil)
	require.ErrorContains(t, err, "exact canonical contracts")
	type similarInput fixture.Input
	type similarOutput fixture.Output
	for _, tc := range []struct {
		name          string
		input, output reflect.Type
		alter         func(*spec.Component)
	}{
		{"absent input", nil, reflect.TypeFor[fixture.Output](), nil},
		{"absent output", reflect.TypeFor[fixture.Input](), nil, nil},
		{"wrong input", reflect.TypeFor[fixture.Row](), reflect.TypeFor[fixture.Output](), nil},
		{"wrong output", reflect.TypeFor[fixture.Input](), reflect.TypeFor[fixture.Row](), nil},
		{"similar input", reflect.TypeFor[similarInput](), reflect.TypeFor[fixture.Output](), nil},
		{"similar output", reflect.TypeFor[fixture.Input](), reflect.TypeFor[similarOutput](), nil},
		{"foreign package", reflect.TypeFor[fixture.Input](), reflect.TypeFor[fixture.Output](), func(c *spec.Component) { c.Key.Scope = "example.com/foreign" }},
		{"foreign route", reflect.TypeFor[fixture.Input](), reflect.TypeFor[fixture.Output](), func(c *spec.Component) { c.Routes[0].Path = "/foreign" }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			candidate := component.Clone()
			if tc.alter != nil {
				tc.alter(candidate)
			}
			_, err := ResolveReportSourceWithContracts(candidate, tc.input, tc.output, nil)
			require.Error(t, err)
		})
	}
}
