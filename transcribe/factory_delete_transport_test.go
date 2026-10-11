package transcribe

import (
	"database/sql"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/viant/datly/transcribe/column"
)

func TestGeneratedFactoryDeleteCurrentOnly(t *testing.T) {
	for _, qualified := range []bool{false, true} {
		t.Run(map[bool]string{false: "bare", true: "qualified"}[qualified], func(t *testing.T) {
			root, source := generatedPostFactoryFixture(t)
			writeSourceHandlerFile(t, root, "archive/business.go", `package archive
import("context";"github.com/viant/xdatly/handler")
type Handler struct{}
func NewArchive() handler.Contract[ArchiveInput,ArchiveOutput] { return &Handler{} }
func (*Handler) Exec(_ context.Context,_ handler.Session,in *ArchiveInput,out *ArchiveOutput)error { out.Rows=in.Current;return nil }
`)
			source.Text = `#package('github.com/viant/datly/handlerfixture/archive')
#import('archive','github.com/viant/datly/handlerfixture/archive')
#setting($_ = $route('/records/{owner}','DELETE'))
#setting($_ = $internal(true))
#setting($_ = $connector('fixture'))
#setting($_ = $handler_factory('archive.NewArchive','Archive'))
#setting($_ = $input_type('archive.ArchiveInput'))
#setting($_ = $output_type('archive.ArchiveOutput'))
#define($_ = $Owner<int>(path/owner))
#define($_ = $Current<?>(view/Current).Cardinality('Many').TypeName('Row') /* SELECT ID,OWNER FROM RECORDS WHERE OWNER=$Owner */)
#define($_ = $Rows<[]*Row>(output/body))`
			if !qualified {
				source.Text = strings.ReplaceAll(source.Text, "archive.Archive", "Archive")
				source.Text = strings.ReplaceAll(source.Text, "archive.NewArchive", "NewArchive")
			}
			db, err := sql.Open("sqlite3", filepath.Join(t.TempDir(), "read.db"))
			require.NoError(t, err)
			defer db.Close()
			_, err = db.Exec("CREATE TABLE RECORDS(ID INTEGER PRIMARY KEY,OWNER INTEGER NOT NULL,UNRELATED TEXT)")
			require.NoError(t, err)
			source.ColumnRefiner = column.New(column.Connections{"fixture": db})
			var stable map[string]string
			for round := 0; round < 3; round++ {
				if round == 2 {
					_, err = db.Exec("ALTER TABLE RECORDS ADD COLUMN EXTRA TEXT")
					require.NoError(t, err)
				}
				compiled, err := (&Discovery{BaseDir: root, GoBuild: source.GoBuild, ColumnRefiner: source.ColumnRefiner}).CompileSource(t.Context(), source)
				require.NoError(t, err)
				require.Nil(t, compiled.Component.RootView)
				require.Nil(t, compiled.ExternalHandler.InputShape)
				require.Empty(t, compiled.Component.Settings.Mutation)
				require.Len(t, compiled.Component.Views, 1)
				require.Len(t, compiled.Component.Views[0].Columns, 2)
				require.Equal(t, "DELETE", compiled.Component.Routes[0].Method)
				require.True(t, compiled.Component.Routes[0].Internal)
				request := GenerationRequest{Compiled: compiled, Destination: root}
				if round == 1 {
					request = GenerationRequest{Source: compiled.Source, Destination: root}
				}
				generated, err := (Generator{Operation: "post"}).Generate(t.Context(), request)
				require.NoError(t, err)
				require.Nil(t, generated.Result.Plan.MutationHandler)
				snapshot := sourceHandlerSnapshot(t, root)
				if round == 0 {
					stable = snapshot
				} else {
					require.Equal(t, stable, snapshot)
				}
				for _, op := range []string{"get", "put", "patch", "handler"} {
					_, err = (Generator{Operation: op}).Generate(t.Context(), GenerationRequest{Compiled: compiled, Destination: root})
					require.Error(t, err)
					require.Equal(t, snapshot, sourceHandlerSnapshot(t, root))
				}
			}
		})
	}
}
