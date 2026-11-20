package transcribe

import (
	"database/sql"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/viant/datly/transcribe/column"
)

func TestFactoryBodyIndependentReadPackageDestination(t *testing.T) {
	root, source := factoryInputShapeFixture(t)
	db, err := sql.Open("sqlite3", filepath.Join(t.TempDir(), "schema.db"))
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, db.Close()) })
	_, err = db.Exec("CREATE TABLE CI_AUDIENCE(TARGET TEXT, EXCLUSION TEXT); CREATE TABLE CANDIDATES(ID INTEGER NOT NULL)")
	require.NoError(t, err)
	source.ColumnRefiner = column.New(column.Connections{"shape-discovery-only": db})
	const declaration = "#define($_ = $Candidates<?>(view/Candidates).Cardinality('Many').TypeName('Candidate')"
	source.Text = strings.Replace(source.Text, "#define($_ = $Status", declaration+" /* SELECT * FROM CANDIDATES */)\n#define($_ = $Status", 1)
	discovery := &Discovery{BaseDir: root, GoBuild: source.GoBuild, ColumnRefiner: source.ColumnRefiner}
	generate := func() {
		t.Helper()
		compiled, err := discovery.CompileSource(t.Context(), source)
		require.NoError(t, err)
		_, err = (Generator{Operation: "post"}).Generate(t.Context(), GenerationRequest{Compiled: compiled, Destination: root})
		require.NoError(t, err)
	}
	generate()
	before := sourceHandlerSnapshot(t, root)
	source.Text = strings.Replace(source.Text, declaration, declaration+".Dest('archive/model/current_rows.go')", 1)
	source.Text = strings.Replace(source.Text, "#setting", "#import('idx','"+handlerFixtureModule+"/archive/index')\n#setting($_ = $indexes_package('idx'))\n#setting", 1)
	generate()
	after := sourceHandlerSnapshot(t, root)
	row := filepath.Join(root, "archive/model/current_rows.go")
	require.Contains(t, after[row], "type Candidate struct")
	require.Contains(t, after[filepath.Join(root, "archive/index/indexes.go")], "package index")
	business := filepath.Join(root, "archive/business.go")
	require.Equal(t, before[business], after[business])
	body := filepath.Join(root, "archive/res.go")
	for _, name := range []string{"Res", "ResHas"} {
		original := generatedStructBlock(before[body], name)
		require.NotEmpty(t, original)
		require.Equal(t, original, generatedStructBlock(after[body], name))
	}
	generate()
	require.Equal(t, after, sourceHandlerSnapshot(t, root))
	_, err = db.Exec("ALTER TABLE CANDIDATES ADD COLUMN REVISION TEXT")
	require.NoError(t, err)
	generate()
	require.Contains(t, sourceHandlerSnapshot(t, root)[row], "Revision")

	// The build must see the proposed model and preserve the last complete
	// output when a schema change would break the application factory.
	businessSource, err := os.ReadFile(business)
	require.NoError(t, err)
	writeSourceHandlerFile(t, root, "archive/business.go", string(businessSource)+"\nfunc useRevision(input *ArchiveInput) { _ = input.Candidates[0].Revision }\n")
	_, err = db.Exec("ALTER TABLE CANDIDATES RENAME COLUMN REVISION TO EDITION")
	require.NoError(t, err)
	lastGood := sourceHandlerSnapshot(t, root)
	compiled, err := discovery.CompileSource(t.Context(), source)
	require.NoError(t, err)
	_, err = (Generator{Operation: "post"}).Generate(t.Context(), GenerationRequest{Compiled: compiled, Destination: root})
	require.ErrorContains(t, err, "Revision")
	require.Equal(t, lastGood, sourceHandlerSnapshot(t, root))
}
