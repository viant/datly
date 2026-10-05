package column

import (
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/viant/datly/spec"
)

func TestRequiredColumnSurvivesDiscovery(t *testing.T) {
	for _, required := range []bool{false, true} {
		base := &spec.Column{Name: "value", Required: required, Tag: `json:"value"`}
		discovered := &spec.Column{Name: "value", Type: spec.TypeRef{Name: "string"}, Nullable: true}
		got := mergeColumns([]*spec.Column{base}, []*spec.Column{discovered})
		require.Len(t, got, 1)
		require.Equal(t, required, got[0].Required)
		require.True(t, got[0].Nullable, "SQL nullability survives required shape")
		require.Equal(t, !required, got[0].EffectiveType().Pointer)
		require.False(t, got[0].NotNull)
		require.Equal(t, base.Tag, got[0].Tag)
		require.True(t, discovered.Nullable)
		require.True(t, base.Type.IsZero())
	}
}

func TestDiscoveryNullabilityIndependentOfAuthoredShape(t *testing.T) {
	for _, nullable := range []bool{false, true} {
		for _, shape := range []string{"default", "required", "optional", "cast-value", "cast-pointer"} {
			base := &spec.Column{Name: "Renamed", Source: "VALUE", Nullable: !nullable, Type: spec.TypeRef{Name: "string"}}
			switch shape {
			case "required":
				base.Required = true
			case "optional":
				base.Optional = true
			case "cast-value":
				base.ExplicitType = true
			case "cast-pointer":
				base.ExplicitType = true
				base.Type.Pointer = true
			}
			snapshot := base.Clone()
			discovered := &spec.Column{Name: "VALUE", Nullable: nullable, NotNull: !nullable, Type: spec.TypeRef{Name: "string"}}
			columns := []*spec.Column{base}
			for pass := 0; pass < 2; pass++ {
				identities, err := resolveResultSources(columns, nil)
				require.NoError(t, err)
				columns = mergeColumnsWithSources(columns, []*spec.Column{discovered}, identities)
				require.Len(t, columns, 1)
				require.Equal(t, nullable, columns[0].Nullable, shape)
				require.Equal(t, !nullable, columns[0].NotNull, shape)
				require.Equal(t, "Renamed", columns[0].Name)
				require.Equal(t, "VALUE", columns[0].Source)
				require.Equal(t, base.Required, columns[0].Required)
				require.Equal(t, base.Optional, columns[0].Optional)
				wantPointer := shape == "optional" || shape == "cast-pointer" || (shape == "default" && nullable)
				require.Equal(t, wantPointer, columns[0].EffectiveType().Pointer)
			}
			require.Equal(t, snapshot, base, "refinement mutated authored metadata")
		}
	}
}
