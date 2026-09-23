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
		require.Equal(t, !required, got[0].Nullable)
		require.Equal(t, !required, got[0].EffectiveType().Pointer)
		require.False(t, got[0].NotNull)
		require.Equal(t, base.Tag, got[0].Tag)
		require.True(t, discovered.Nullable)
		require.True(t, base.Type.IsZero())
	}
}
