package transcribe

import (
	"database/sql"
	"path/filepath"
	"strings"
	"testing"
	"testing/fstest"

	"github.com/stretchr/testify/require"
	"github.com/viant/bindly/resource"
	"github.com/viant/datly/transcribe/column"
)

func TestFactoryEmbeddedCurrentMaterializedAnnotations(t *testing.T) {
	root, source := generatedPostFactoryFixture(t)
	db, err := sql.Open("sqlite3", filepath.Join(t.TempDir(), "read.db"))
	require.NoError(t, err)
	defer db.Close()
	_, err = db.Exec(`CREATE TABLE records(id INTEGER PRIMARY KEY,owner INTEGER NOT NULL); CREATE TABLE owners(id INTEGER PRIMARY KEY);`)
	require.NoError(t, err)
	source.Connector = "read"
	source.ColumnRefiner = column.New(column.Connections{"read": db})
	original := `SELECT DISTINCT r.id,r.owner FROM records r JOIN owners o ON o.id=r.owner WHERE r.owner=$Owner AND NOT EXISTS(SELECT 1 FROM records blocked WHERE blocked.id=r.id AND blocked.owner=99)`
	resources := resource.New()
	require.NoError(t, resources.Register("", fstest.MapFS{"sql/candidates.sql": &fstest.MapFile{Data: []byte(original)}}))
	source.Resources = resources
	source.Text = strings.Replace(source.Text, "#define($_ = $Data<*archive.ArchiveRequest>(body/data).Optional())", `#define($_ = $Owner<int>(query/owner).Optional())
#define($_ = $Current<?>(view/Current).Cardinality('Many').TypeName('CurrentRow') /* SELECT candidates.*,CAST(candidates.id AS int),CAST(candidates.owner AS int),allow_nulls(candidates) FROM (${embed:sql/candidates.sql}) candidates */)`, 1)
	compiled, err := (&Discovery{BaseDir: root, GoBuild: source.GoBuild, ColumnRefiner: source.ColumnRefiner}).CompileSource(t.Context(), source)
	require.NoError(t, err)
	require.Nil(t, compiled.Component.RootView)
	require.Nil(t, compiled.ExternalHandler.InputShape)
	require.Len(t, compiled.Component.Views, 1)
	require.Len(t, compiled.Component.Views[0].Columns, 2)
	require.Empty(t, compiled.Component.Views[0].Source.Embeds, "materialized SQL must not retain consumed embed tokens")
	require.Contains(t, compiled.Component.Views[0].Source.SQL, "DISTINCT")
	require.Contains(t, compiled.Component.Views[0].Source.SQL, "NOT EXISTS")
	require.NotContains(t, compiled.Component.Views[0].Source.SQL, "CAST(", "authoring CAST must be lowered")
}
