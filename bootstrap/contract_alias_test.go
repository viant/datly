package bootstrap

import (
	"reflect"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/viant/datly/spec"
	"github.com/viant/datly/tag"
	"github.com/viant/x"
)

type aliasContractInput struct{ ID int }
type aliasContractOutput struct{ Value string }
type aliasContractDistinct aliasContractOutput

func TestLinkedContractSourceAliases(t *testing.T) {
	input, output := reflect.TypeFor[aliasContractInput](), reflect.TypeFor[aliasContractOutput]()
	pkg := output.PkgPath()
	imports := map[string]string{"original": pkg}
	types := predicateContextResolver(t,
		x.NewType(input), x.NewType(output), x.NewType(reflect.TypeFor[aliasContractDistinct]()),
		predicateContextType(t, pkg, "CubeOutput", "= aliasContractOutput", nil),
		predicateContextType(t, pkg, "InputAlias", "= aliasContractInput", nil),
		predicateContextType(t, pkg, "Chain", "= CubeOutput", nil),
		predicateContextType(t, "example.com/contracts", "Imported", "= *original.aliasContractOutput", imports),
		predicateContextType(t, pkg, "CycleA", "= CycleB", nil),
		predicateContextType(t, pkg, "CycleB", "= CycleA", nil),
	)
	for _, tc := range []struct {
		name, expression string
		actual           reflect.Type
		valid            bool
	}{
		{"direct", "aliasContractOutput", output, true},
		{"output alias", "CubeOutput", output, true},
		{"chain", "Chain", output, true},
		{"pointer", "*Chain", reflect.PointerTo(output), true},
		{"slice", "[]Chain", reflect.SliceOf(output), true},
		{"map", "map[string]*Chain", reflect.MapOf(reflect.TypeFor[string](), reflect.PointerTo(output)), true},
		{"imported alias", "shared.Imported", reflect.PointerTo(output), true},
		{"wrong pointer", "CubeOutput", reflect.PointerTo(output), false},
		{"distinct named type", "CubeOutput", reflect.TypeFor[aliasContractDistinct](), false},
		{"distinct declaration", "aliasContractDistinct", output, false},
		{"unknown alias", "Missing", output, false},
		{"cycle", "CycleA", output, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			route := &RouteSource{PackagePath: pkg, InputType: "InputAlias", OutputType: tc.expression,
				Imports: []spec.ImportSpec{{Alias: "shared", Package: "example.com/contracts"}, {Alias: "original", Package: "example.com/wrong"}},
			}
			err := route.validateContractTypes(input, tc.actual, types)
			if tc.valid {
				require.NoError(t, err)
			} else {
				require.Error(t, err)
			}
		})
	}
	// The grouping path used by the source index must pass the catalog into
	// validation. A linked-only caller must not guess alias identity.
	route := &RouteSource{PackagePath: pkg, InputType: "InputAlias", OutputType: "CubeOutput",
		LinkedInputType: input, LinkedOutputType: output,
		Tag: tag.Component{Name: "Cube", Path: "/cube", Method: "POST"},
	}
	require.Error(t, route.ValidateContractTypes(input, output))
	groups, err := GroupPackageComponentSources([]*RouteSource{route}, types)
	require.NoError(t, err)
	require.Len(t, groups, 1)
	require.Equal(t, output, groups[0].OutputType.Type)
}
