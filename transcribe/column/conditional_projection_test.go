package column

import (
	"testing"
	"testing/fstest"

	"github.com/stretchr/testify/require"
	"github.com/viant/datly/spec"
)

func TestStaticConditionalProjectionPreservesEmbeddedSQL(t *testing.T) {
	for _, query := range []string{
		"SELECT id, name FROM records WHERE 1=1 #if($ID) AND id=$ID #end",
		"SELECT id, name FROM records #if($IncludeArchive) UNION ALL SELECT id, name FROM archived #end",
		"SELECT r.id, r.name FROM records r #if($Join) JOIN labels l ON l.id=r.id #end",
	} {
		component := &spec.Component{RootView: &spec.View{Name: "Records", Namespace: "r", Source: &spec.ViewSource{URI: "sql/records.sql"}, Columns: []*spec.Column{{Name: "id", Source: "id", Type: spec.TypeRef{Name: "int"}, ExplicitType: true}}}}
		files := fstest.MapFS{"sql/records.sql": &fstest.MapFile{Data: []byte(query)}}
		require.NoError(t, New(Connections{}).ValidateSourceProjections(component, files), query)
		require.Equal(t, "sql/records.sql", component.RootView.Source.URI)
		require.Empty(t, component.RootView.Source.SQL)
		require.Equal(t, query, string(files["sql/records.sql"].Data))
	}
}
func TestStaticConditionalProjectionChecksEveryBranch(t *testing.T) {
	component := &spec.Component{RootView: &spec.View{Name: "Records", Namespace: "r", Source: &spec.ViewSource{SQL: "SELECT #if($Flag) id #else name #end FROM records"}, Columns: []*spec.Column{{Name: "id", Source: "id", Type: spec.TypeRef{Name: "int"}, ExplicitType: true}}}}
	require.Error(t, New(Connections{}).ValidateSourceProjections(component, nil))
}
