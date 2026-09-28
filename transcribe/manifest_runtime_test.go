package transcribe

import (
	"context"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/viant/datly/internal/testharness"
	"github.com/viant/datly/internal/testharness/manifestfree"
	"github.com/viant/datly/transcribe/column"
)

// Each use case independently hydrates a module/database, exercises the public
// API, regenerates edited output, builds, and invokes the real SQLite runtime.
func TestManifestFreeGenerationRuntime(t *testing.T) {
	type usecase struct{ name, operation, legacy string }
	cases := []usecase{{"reader", "get", ""}, {"insert", "post", "{broken"}, {"replace", "put", `{"owner":"Wrong"}`}, {"sparse", "patch", ""}}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			ctx := context.Background()
			db := testharness.NewSQLiteHarness(t)
			require.NoError(t, db.ExecStatements(ctx, "CREATE TABLE records(id INTEGER PRIMARY KEY,name TEXT, extra TEXT)"))
			root := t.TempDir()
			(testharness.GeneratedModule{Path: "github.com/viant/datly/testfixture/manifestfree"}).Write(t, root)
			source := &Source{Name: "Records", Scope: "example.com/generated/source", Connector: "main", ColumnRefiner: column.New(column.Connections{"main": db.DB}), Text: fmt.Sprintf(`#package('records')
#setting($_ = $route('/records','%s'))
#setting($_ = $case_format('lc'))
#setting($_ = $input_type('RecordsInput'))
#setting($_ = $output_type('RecordsOutput'))
#define($_ = $Data<[]*Record>(output/body))
SELECT r.id,r.name,r.extra,type(r,'Record'),CAST(r.id AS int),CAST(r.name AS string),CAST(r.extra AS string),tag(r.id,'sqlx:"id,primaryKey"') FROM records r`, strings.ToUpper(tc.operation))}
			if tc.operation == "get" {
				source.Text = strings.Replace(source.Text, "(output/body)", "(output/view)", 1)
			}
			generator := Generator{Operation: tc.operation}
			request := GenerationRequest{Source: source, Destination: root}
			first, err := generator.Generate(ctx, request)
			require.NoError(t, err)
			directory := filepath.Join(root, "records")
			if tc.legacy != "" {
				require.NoError(t, os.WriteFile(filepath.Join(directory, ".datly-gen.json"), []byte(tc.legacy), 0644))
			}
			hooks := "package records\nimport \\\"context\\\"\nvar ApplicationInitCalls int\nfunc(*RecordsInput)Init(context.Context)error{ApplicationInitCalls++;return nil}\n"
			hooks = strings.ReplaceAll(hooks, `\"`, `"`)
			require.NoError(t, os.WriteFile(filepath.Join(directory, "hooks.go"), []byte(hooks), 0644))
			// DQL removes Extra and changes nullability after a direct generated edit.
			shape := filepath.Join(directory, first.Result.Plan.ViewDest)
			data, err := os.ReadFile(shape)
			require.NoError(t, err)
			require.NoError(t, os.WriteFile(shape, append(data, []byte("\n// direct generated edit\n")...), 0644))
			source.Text = strings.ReplaceAll(source.Text, "r.id,r.name,r.extra", "r.id,r.name")
			source.Text = strings.ReplaceAll(source.Text, ",CAST(r.extra AS string)", "")
			source.Text = strings.ReplaceAll(source.Text, "CAST(r.name AS string)", "CAST(r.name AS *string)")
			generated, err := generator.Generate(ctx, request)
			require.NoError(t, err)
			_, err = generator.Generate(ctx, request)
			require.NoError(t, err)
			data, err = os.ReadFile(shape)
			require.NoError(t, err)
			require.NotContains(t, string(data), "direct generated edit")
			require.NotContains(t, string(data), "Extra ")
			data, err = os.ReadFile(filepath.Join(directory, "hooks.go"))
			require.NoError(t, err)
			require.Equal(t, hooks, string(data))
			require.NoError(t, filepath.WalkDir(root, func(path string, entry os.DirEntry, err error) error {
				if err != nil {
					return err
				}
				require.NotEqual(t, ".datly-gen.json", entry.Name())
				return nil
			}))
			runtime := strings.NewReplacer("HOLDER", generated.Result.Plan.HolderName(), "OPERATION", tc.operation, "METHOD", strings.ToUpper(tc.operation)).Replace(manifestfree.RuntimeSource)
			require.NoError(t, os.WriteFile(filepath.Join(directory, "runtime_test.go"), []byte(runtime), 0644))
			cmd := exec.CommandContext(ctx, "go", "test", "-mod=mod", "-count=1", "./...")
			cmd.Dir = root
			output, err := cmd.CombinedOutput()
			require.NoError(t, err, "%s", output)
		})
	}
}
