package transcribe

import (
	"database/sql"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/viant/datly/transcribe/column"
)

// Generated factory contracts must not become linked discovery inputs when a
// dependency has the same conventional names, or when old generated Go exists.
func TestFactoryInputDiscoveryGeneratedContractAuthority(t *testing.T) {
	t.Run("body graph", func(t *testing.T) { testFactoryInputDiscoveryGeneratedContractAuthority(t, true) })
	t.Run("independent reads only", func(t *testing.T) { testFactoryInputDiscoveryGeneratedContractAuthority(t, false) })
}

func testFactoryInputDiscoveryGeneratedContractAuthority(t *testing.T, body bool) {
	root, source := generatedPostFactoryFixture(t)
	writeSourceHandlerFile(t, root, "dependency/types.go", `package dependency
 type Input struct { Ignored string }
 type Output struct { Value string }
 `)
	writeSourceHandlerFile(t, root, "archive/business.go", `package archive
 import("context";"github.com/viant/xdatly/handler";dependency "`+handlerFixtureModule+`/dependency")
 type Handler struct{}
 var dependencyInput dependency.Input
 func NewArchive() handler.Contract[Input,Output] { return &Handler{} }
 func(*Handler) Exec(_ context.Context,_ handler.Session,in *Input,out *Output)error{out.Status="ok";out.Results=[]*dependency.Output{{Value:"ok"}};return nil}
 `)
	source.Text = fmt.Sprintf(`#package(%q)
#import('archive',%q)
#import('dependency',%q)

#setting($_ = $handler_factory('archive.NewArchive','Archive'))
#setting($_ = $route('/records','PATCH'))
#setting($_ = $connector('discovery'))
#setting($_ = $DocURLs('documentation.yaml'))
#setting($_ = $input_type('Input'))
#setting($_ = $output_type('Output'))

#define($_ = $Data<[]*Record>(body/data).Optional())
#define($_ = $Status<string>(output/status))
#define($_ = $Results<[]*dependency.Output>(output/body))
SELECT r.*,type(r,'Record'),dest(r,'record.go') FROM records r`, handlerFixtureModule+"/archive", handlerFixtureModule+"/archive", handlerFixtureModule+"/dependency")
	if !body {
		source.Text = strings.Replace(source.Text, "#define($_ = $Data<[]*Record>(body/data).Optional())", `#define($_ = $Current<?>(view/Current).Cardinality('Many').TypeName('CurrentRow').Dest('current.go') /* SELECT * FROM records */)`, 1)
		source.Text = strings.Replace(source.Text, "SELECT r.*,type(r,'Record'),dest(r,'record.go') FROM records r", "", 1)
	}
	writeSourceHandlerFile(t, root, "dql/documentation.yaml", "parameters:\n  Label: Record label filter\n")
	source.Connector = "discovery"
	db, err := sql.Open("sqlite3", filepath.Join(t.TempDir(), "discovery.db"))
	require.NoError(t, err)
	defer db.Close()
	_, err = db.Exec("CREATE TABLE records(ID INTEGER PRIMARY KEY, NAME TEXT)")
	require.NoError(t, err)
	source.ColumnRefiner = column.New(column.Connections{"discovery": db})
	require.NotEqual(t, source.Scope, handlerFixtureModule+"/archive")
	for evolution := 0; evolution < 2; evolution++ {
		if evolution == 1 {
			_, err = db.Exec("ALTER TABLE records ADD COLUMN EXTRA TEXT")
			require.NoError(t, err)
			if !body {
				source.Text = strings.Replace(source.Text, `/* SELECT * FROM records */`, `/* SELECT * FROM records WHERE NAME=$Label */`, 1)
			} else {
				source.Text = strings.Replace(source.Text, "#define($_ = $Status", `#define($_ = $ExtraKeys<?>(param/Data) /* ? SELECT ARRAY_AGG(Extra) AS Values FROM `+"`/`"+` LIMIT 1 */)
#define($_ = $Current<?>(view/Current).Cardinality('Many').TypeName('CurrentRow').Dest('current.go') /* SELECT * FROM records WHERE $criteria.In("EXTRA",$ExtraKeys.Values) AND NAME=$Label */)
#define($_ = $Status`, 1)
			}
			source.Text = strings.Replace(source.Text, "#define($_ = $Status", `#define($_ = $Label<string>(query/label).Optional())
#define($_ = $Status`, 1)
		}
		writeSourceHandlerFile(t, root, "dql/records.dql", source.Text)
		var first map[string]string
		for pass := 0; pass < 2; pass++ {
			project, e := (&Discovery{BaseDir: root, Include: []string{handlerFixtureModule + "/dql"}, GoBuild: source.GoBuild, ColumnRefiner: source.ColumnRefiner}).Compile(t.Context())
			require.NoError(t, e)
			require.Len(t, project.Components, 1)
			compiled := project.Components[0]
			require.Nil(t, compiled.Component.RootView)
			require.Empty(t, compiled.Component.Settings.Mutation)
			require.Equal(t, "Input", compiled.Component.Settings.InputType)
			require.Equal(t, "Output", compiled.Component.Settings.OutputType)
			shape := compiled.ExternalHandler.InputShape
			if body {
				require.NotNil(t, shape)
				require.Len(t, shape.Columns, 2+evolution)
				require.Len(t, compiled.Component.Views, evolution)
			} else {
				require.Nil(t, shape)
				require.Len(t, compiled.Component.Views, 1)
				require.Len(t, compiled.Component.Views[0].Columns, 2+evolution)
			}
			generated, e := (Generator{Operation: "post"}).Generate(t.Context(), GenerationRequest{Compiled: compiled, Destination: root})
			require.NoError(t, e)
			require.Nil(t, generated.Result.Plan.MutationHandler)
			require.Empty(t, generated.Result.Plan.RootSource)
			require.Equal(t, []string{"documentation.yaml"}, compiled.Component.Documentation.DocURLs)
			require.NotNil(t, compiled.Source.Resources)
			require.Len(t, generated.Result.Plan.Documentation.DocURLs, 1)
			require.Contains(t, generated.Result.Plan.Documentation.DocURLs[0], "documentation.yaml")
			require.NotNil(t, generated.Result.Plan.Resources)
			var documentFound bool
			for _, asset := range generated.Result.Plan.Resources.Files {
				if strings.HasSuffix(asset.Path, "documentation.yaml") {
					require.Contains(t, asset.Content, "Record label filter")
					documentFound = true
				}
			}
			require.True(t, documentFound)
			require.Equal(t, "Input", generated.Result.Plan.Input.Type)
			require.Equal(t, "Output", generated.Result.Plan.Output.Type)
			input, e := os.ReadFile(filepath.Join(root, "archive/input.go"))
			require.NoError(t, e)
			output, e := os.ReadFile(filepath.Join(root, "archive/output.go"))
			require.NoError(t, e)
			require.Contains(t, string(output), "[]*dependency.Output")
			if evolution == 1 {
				require.Contains(t, string(input), "Label")
				require.Contains(t, string(input), "Current")
				if body {
					require.Contains(t, string(input), "ExtraKeys")
					require.Contains(t, sourceHandlerSnapshot(t, root)[filepath.Join(root, "archive/record.go")], "Extra")
				} else {
					require.Contains(t, sourceHandlerSnapshot(t, root)[filepath.Join(root, "archive/current.go")], "Extra")
				}
			}
			now := sourceHandlerSnapshot(t, root)
			if pass == 0 {
				first = now
			} else {
				require.Equal(t, first, now)
			}
		}
	}
}
