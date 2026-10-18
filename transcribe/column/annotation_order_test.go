package column

import (
	"github.com/stretchr/testify/require"
	"github.com/viant/datly/spec"
	"testing"
)

func TestAnnotationOrderPreservesCanonicalOwners(t *testing.T) {
	for _, inferred := range []bool{false, true} {
		t.Run(map[bool]string{false: "explicit", true: "linked"}[inferred], func(t *testing.T) {
			base := []*spec.Column{{Name: "b", Source: "b", NameInferred: inferred}, {Name: "c", Source: "c", AnnotationOnly: true, Optional: true}, {Name: "a", Source: "a", NameInferred: inferred}}
			fresh := []*spec.Column{{Name: "a"}, {Name: "b"}, {Name: "c", Type: spec.TypeRef{Name: "string"}}}
			ids, err := resolveResultSources(base, nil)
			require.NoError(t, err)
			got := mergeColumnsWithSources(base, fresh, ids)
			require.Equal(t, []string{"b", "a", "c"}, []string{got[0].Name, got[1].Name, got[2].Name})
			require.True(t, got[2].Optional)
			require.True(t, got[2].EffectiveType().Pointer)
			require.False(t, got[2].AnnotationOnly)
			require.True(t, base[1].AnnotationOnly)
		})
	}
}

func TestUnmatchedAnnotationPreservesBasePosition(t *testing.T) {
	base := []*spec.Column{{Name: "absent", Source: "absent", AnnotationOnly: true, Optional: true}}
	ids, err := resolveResultSources(base, nil)
	require.NoError(t, err)
	got := mergeColumnsWithSources(base, []*spec.Column{{Name: "id"}}, ids)
	require.Len(t, got, 2)
	require.Equal(t, "absent", got[0].Name)
	require.True(t, got[0].Optional)
	require.NotSame(t, base[0], got[0])
}
