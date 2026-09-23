package generate

import (
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/viant/datly/spec"
)

func TestMapColumnImportsAndWrappers(t *testing.T) {
	for _, tc := range []struct {
		name string
		typ  spec.TypeRef
		want string
	}{
		{"canonical packages", spec.TypeRef{Name: "map[example.com/keys.ID][]*example.com/values.T"}, "map[key.ID][]*value.T"},
		{"authored aliases", spec.TypeRef{Name: "map[key.ID][]*value.T"}, "map[key.ID][]*value.T"},
		{"pointer to map", spec.TypeRef{Name: "map[string][]string", Pointer: true}, "*map[string][]string"},
		{"slice of maps", spec.TypeRef{Name: "map[string][]string", Cardinality: spec.CardinalityMany}, "[]map[string][]string"},
		{"map value pointer to slice", spec.TypeRef{Name: "map[string]*[]example.com/values.T"}, "map[string]*[]value.T"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			imports := []spec.ImportSpec{{Alias: "key", Package: "example.com/keys"}, {Alias: "value", Package: "example.com/values"}}
			plan := &Plan{Imports: append([]spec.ImportSpec(nil), imports...)}
			view := &spec.View{Name: "maps", Columns: []*spec.Column{{Name: "values", Type: tc.typ, Nullable: true}}}
			fields, err := resolveScalarViewFields(plan, view, false)
			require.NoError(t, err)
			require.Len(t, fields, 1)
			require.Equal(t, tc.want, fields[0].Type)
			require.Equal(t, imports, plan.Imports)
		})
	}
}
