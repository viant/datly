package tool

import (
	"reflect"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/viant/bindly"
	bindstate "github.com/viant/bindly/state"
	"github.com/viant/datly/spec"
)

func TestExposureStrictArgumentsOverridesWithoutChangingCompilerDefault(t *testing.T) {
	type payload struct{ Name string }
	contract := testRouteContract(t, reflect.TypeFor[payload](), []bindly.BindingSpec{{Path: "Name", Name: "Name", Location: bindstate.Location{Kind: "query", In: "name"}}})
	yes, no := true, false
	for _, global := range []bool{false, true} {
		compiler := NewCompiler(WithStrictArguments(global))
		for _, tc := range []struct {
			name     string
			override *bool
			strict   bool
		}{
			{"inherit", nil, global}, {"opt_in", &yes, true}, {"opt_out", &no, false}, {"inherit_again", nil, global},
		} {
			t.Run(tc.name+map[bool]string{false: "_lenient_host", true: "_strict_host"}[global], func(t *testing.T) {
				plan, err := compiler.Compile(Input{Contract: contract, Exposure: &spec.MCPExposure{Kind: spec.MCPExposureTool, Name: tc.name, StrictArguments: tc.override}})
				require.NoError(t, err)
				scope, err := plan.Scope(map[string]any{"name": "kept", "dryRun": true})
				if tc.strict {
					require.ErrorContains(t, err, "unknown MCP argument")
					return
				}
				require.NoError(t, err)
				defer scope.Close()
				value, present, err := scope.Query().Locate(nil).Value(t.Context(), reflect.TypeFor[string](), "name")
				require.NoError(t, err)
				require.True(t, present)
				require.Equal(t, "kept", value)
			})
		}
	}
}
