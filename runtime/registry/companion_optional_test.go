package registry_test

import (
	"reflect"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/viant/datly/bootstrap"
	fixture "github.com/viant/datly/bootstrap/testdata/selectorcompanion"
	"github.com/viant/datly/spec"
)

// Trusted prepared input must keep the same public optionalization behavior
// when a reader also declares selector side inputs.
func TestCompanionPreparedInputRetainsSelectors(t *testing.T) {
	for _, required := range []bool{false, true} {
		t.Run(map[bool]string{false: "optional", true: "required"}[required], func(t *testing.T) {
			discovered, err := bootstrap.ReflectSelectedPackages([]string{reflect.TypeFor[fixture.Component]().PkgPath()}, nil)
			require.NoError(t, err)
			require.Len(t, discovered.Components, 1)
			component, err := discovered.Components[0].Resolve(reflect.TypeFor[fixture.Input](), reflect.TypeFor[fixture.Output]())
			require.NoError(t, err)
			for _, p := range component.Parameters {
				if p.Name == "ID" {
					value := required
					p.Required = &value
				}
			}
			artifact, err := bootstrap.BuildArtifact(bootstrap.ArtifactInput{Component: component, InputType: reflect.TypeFor[fixture.Input](), OutputType: reflect.TypeFor[fixture.Output]()})
			require.NoError(t, err)
			route, ok := artifact.Input.ForRoute(spec.RouteRef{Method: "GET", Path: "/selector-records"})
			require.True(t, ok)
			require.NotNil(t, route.SelectorPlan())
			prepared, err := route.WithOptionalBindings("ID")
			require.NoError(t, err)
			if required {
				require.NotSame(t, route, prepared)
			} else {
				require.Same(t, route, prepared)
			}
			require.NotNil(t, prepared.SelectorPlan())
			require.Same(t, prepared.Plan(), prepared.SelectorPlan().PublicPlan())
			require.Equal(t, route.SelectorPlan().Type(), prepared.SelectorPlan().Type())
			require.Len(t, prepared.Fields(), 1)
			require.NotNil(t, prepared.Fields()[0].Binding().Required)
			require.False(t, *prepared.Fields()[0].Binding().Required)
			require.Equal(t, required, *route.Fields()[0].Binding().Required)
			_, err = route.WithOptionalBindings("missing")
			require.Error(t, err)
		})
	}
}
