package tool

import (
	"github.com/stretchr/testify/require"
	"github.com/viant/bindly"
	bindstate "github.com/viant/bindly/state"
	"github.com/viant/datly/runtime/registry"
	"github.com/viant/datly/spec"
	"github.com/xeipuuv/gojsonschema"
	"reflect"
	"testing"
)

type scalarTrimInput struct {
	Channel int `parameter:"Channel,kind=body" choice:"1:DISPLAY" choice:"2:VIDEO" enumAccept:"scalar" enumInteger:"passthrough" enumCase:"upper" enumAlias:"1:1" enumAlias:"2:OLV" enumTrim:"space"`
}

func TestScalarTrimMCPAdmissionAndBinding(t *testing.T) {
	field := reflect.TypeFor[scalarTrimInput]().Field(0)
	binding, err := registry.EnumBinding(bindly.BindingSpec{Path: "Channel", Name: "Channel", Location: bindstate.Location{Kind: "body"}}, field)
	require.NoError(t, err)
	contract := testRouteContract(t, reflect.TypeFor[scalarTrimInput](), []bindly.BindingSpec{binding})
	plan, err := NewCompiler().Compile(Input{Component: spec.Key{Kind: spec.KindComponent, Scope: "test", Name: "Trim"}, Exposure: &spec.MCPExposure{Kind: spec.MCPExposureTool, Name: "trim"}, Contract: contract})
	require.NoError(t, err)
	for _, tc := range []struct {
		value any
		valid bool
		want  int
	}{
		{" DISPLAY ", true, 1}, {"\t1\r\n", true, 1}, {"\u0085\u00a0\u1680\u2000\u200a\u2028\u2029\u202f\u205f\u3000olv\n", true, 2}, {99, true, 99}, {" 99 ", false, 0}, {"\u180eDISPLAY", false, 0}, {"DISPLAY\ufeff", false, 0},
	} {
		args := map[string]any{"Channel": tc.value}
		result, err := gojsonschema.Validate(gojsonschema.NewGoLoader(plan.Metadata().InputSchema), gojsonschema.NewGoLoader(args))
		require.NoError(t, err)
		require.Equal(t, tc.valid, result.Valid(), "%v: %v", tc.value, result.Errors())
		scope, err := plan.Scope(args)
		if !tc.valid && err != nil {
			continue
		}
		require.NoError(t, err)
		injector, err := bindly.NewInjector()
		require.NoError(t, err)
		bound, err := injector.ForScope(scope.Providers()...)
		require.NoError(t, err)
		var actual scalarTrimInput
		err = bound.Bind(t.Context(), &actual, bindly.WithPlan(contract.Plan()))
		scope.Close()
		if tc.valid {
			require.NoError(t, err)
			require.Equal(t, tc.want, actual.Channel)
		} else {
			require.Error(t, err)
		}
	}
}
