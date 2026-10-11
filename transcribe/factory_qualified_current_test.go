package transcribe

import (
	"context"
	"database/sql"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/viant/datly/transcribe/column"
)

func TestFactoryInputShapeQualifiedAuxiliaryCurrentGeneration(t *testing.T) {
	for _, qualifier := range []string{"archive", handlerFixtureModule + "/archive"} {
		t.Run(qualifier, func(t *testing.T) {
			root, source := factoryInputShapeFixture(t)
			db, err := sql.Open("sqlite3", filepath.Join(t.TempDir(), "current.db"))
			require.NoError(t, err)
			defer db.Close()
			_, err = db.Exec("CREATE TABLE CI_AUDIENCE(TARGET TEXT, EXCLUSION TEXT); CREATE TABLE catalog(ID INTEGER PRIMARY KEY, NAME TEXT)")
			require.NoError(t, err)
			source.ColumnRefiner = column.New(column.Connections{"shape-discovery-only": db})
			source.Text = strings.Replace(source.Text, factoryInputProjection, `SELECT a.*, a.TARGET AS TargetLabel, type(a,'Res'), dest(a,'res.go'), required(a.TARGET), required(a.EXCLUSION) FROM CI_AUDIENCE a`, 1)
			businessPath := filepath.Join(root, "archive/business.go")
			business, err := os.ReadFile(businessPath)
			require.NoError(t, err)
			require.NoError(t, os.WriteFile(businessPath, []byte(strings.ReplaceAll(string(business), "value.TargetStr=r.Target;", "if r.TargetLabel!=nil {value.TargetStr=*r.TargetLabel};")), 0600))
			source.Text = strings.ReplaceAll(source.Text, "'ArchiveInput'", "'"+qualifier+".ArchiveInput'")
			source.Text = strings.ReplaceAll(source.Text, "'ArchiveOutput'", "'"+qualifier+".ArchiveOutput'")
			source.Text = strings.ReplaceAll(source.Text, "[]*Res>", "[]*"+qualifier+".Res>")
			source.Text = strings.ReplaceAll(source.Text, "type(a,'Res')", "type(a,'"+qualifier+".Res')")
			source.Text = strings.Replace(source.Text, "#define($_ = $Status", "#define($_ = $Current<?>(view/Current).Cardinality('Many').TypeName('"+qualifier+".CatalogRow') /* SELECT c.* FROM catalog c WHERE ID=7 */)\n#define($_ = $Status", 1)
			var first map[string]string
			for round := 0; round < 3; round++ {
				if round == 2 {
					_, err = db.Exec("ALTER TABLE catalog ADD COLUMN REVISION TEXT")
					require.NoError(t, err)
				}
				compiled, err := (&Discovery{BaseDir: root, GoBuild: source.GoBuild, ColumnRefiner: source.ColumnRefiner}).CompileSource(context.Background(), source)
				require.NoError(t, err)
				_, err = (Generator{Operation: "post"}).Generate(context.Background(), GenerationRequest{Compiled: compiled, Destination: root})
				require.NoError(t, err)
				now := sourceHandlerSnapshot(t, root)
				var emitted string
				for _, contents := range now {
					emitted += contents
				}
				require.Contains(t, emitted, "GetTargetLabel")
				require.NotContains(t, emitted, "func (r *Res) GetTARGET")
				if round == 0 {
					first = now
				} else if round == 1 {
					require.Equal(t, first, now)
				} else {
					var combined string
					for _, contents := range now {
						combined += contents
					}
					require.Contains(t, combined, "Revision")
					require.Equal(t, 1, strings.Count(combined, "type CatalogRow struct"))
				}
			}
			cmd := exec.CommandContext(t.Context(), "go", "test", "-mod=readonly", "./archive")
			cmd.Dir = root
			output, err := cmd.CombinedOutput()
			require.NoError(t, err, "%s", output)
		})
	}
}
