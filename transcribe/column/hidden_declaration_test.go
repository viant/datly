package column

import (
	"github.com/stretchr/testify/require"
	"github.com/viant/datly/spec"
	"testing"
)

func TestHiddenDiscoveryDeclarationIsExactViewAndCompilationScoped(t *testing.T) {
	root := &spec.View{Name: "Root", Source: &spec.ViewSource{SQL: "SELECT r.* FROM (SELECT CAST(value AS CHAR) AS lease_until_raw FROM records) r"}}
	child := &spec.View{Name: "Child"}
	original := &spec.Column{Name: "Lease", NameInferred: true, Source: "lease_until_raw", Type: spec.TypeRef{Name: "string", Pointer: true}, Tag: `internal:"true" json:"-" sqlx:"lease_until_raw"`}
	first := New(nil).BeginCompilation()
	first.DeclareHiddenColumns(root, []*spec.Column{original})
	original.Source = "changed"
	require.Equal(t, "lease_until_raw", first.hidden[root][0].Source)
	require.Empty(t, first.hidden[child])
	require.Empty(t, New(nil).BeginCompilation().hidden[root])
	identities, err := resolveResultSources(first.hidden[root], root.Source)
	require.NoError(t, err)
	names, err := declaredResultColumns(first.hidden[root], identities)
	require.NoError(t, err)
	require.Equal(t, []string{"lease_until_raw"}, names)
	require.Error(t, identities.validateResults(first.hidden[root], []string{"unowned"}))
	require.NoError(t, identities.validateResults(first.hidden[root], []string{"lease_until_raw"}))
	duplicate := first.hidden[root][0].Clone()
	duplicate.Name = "Other"
	_, err = resolveResultSources(append(first.hidden[root], duplicate), root.Source)
	require.ErrorContains(t, err, "ambiguous SQL result ownership")
}
