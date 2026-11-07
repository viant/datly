package column

import (
	"testing"
	"testing/fstest"

	"github.com/stretchr/testify/require"
	"github.com/viant/datly/spec"
)

func TestStaticProjectionPreservesNativeCriteriaFunction(t *testing.T) {
	const query = `SELECT d.* FROM (SELECT COUNT(DISTINCT r.id) AS total FROM records r WHERE $criteria.In("LOWER(r.owner_key)", $OwnerKeys)) d`
	component := &spec.Component{
		Parameters: []*spec.Parameter{{Name: "OwnerKeys", TypeExpr: "[]string"}},
		RootView:   &spec.View{Name: "Counts", Namespace: "d", Source: &spec.ViewSource{URI: "sql/counts.sql"}, Columns: []*spec.Column{{Name: "total", Source: "total", Type: spec.TypeRef{Name: "int"}, ExplicitType: true}}},
	}
	files := fstest.MapFS{"sql/counts.sql": &fstest.MapFile{Data: []byte(query)}}
	require.NoError(t, New(Connections{}).ValidateSourceProjections(component, files))
	require.Equal(t, "sql/counts.sql", component.RootView.Source.URI)
	require.Empty(t, component.RootView.Source.SQL)
	require.Equal(t, query, string(files["sql/counts.sql"].Data))
}
