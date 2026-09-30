package main

import (
	"bytes"
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"os/exec"

	"github.com/stretchr/testify/require"
	"github.com/viant/datly/internal/testharness"
	"github.com/viant/datly/internal/testharness/manifestfree"
)

func TestTranscriptionRemovesLegacyManifest(t *testing.T) {
	for _, operation := range []string{"get", "post", "put", "patch"} {
		for _, legacy := range []string{"absent", "valid", "malformed"} {
			t.Run(operation+"/"+legacy, func(t *testing.T) {
				root := t.TempDir()
				const module = "github.com/viant/datly/testfixture/manifestfreecli"
				(testharness.GeneratedModule{Path: module}).Write(t, root)
				require.NoError(t, os.Mkdir(filepath.Join(root, "source"), 0700))
				source := fmt.Sprintf(`#package('records')
#setting($_ = $case_format('lc'))
#setting($_ = $route('/records','%s'))
#setting($_ = $input_type('RecordsInput'))
#setting($_ = $output_type('RecordsOutput'))
#define($_ = $Data<[]*Record>(output/body))
SELECT r.ID, r.NAME, type(r,'Record'),
CAST(r.ID AS int), CAST(r.NAME AS string), tag(r.ID,'sqlx:"ID,primaryKey"')
FROM records r`, strings.ToUpper(operation))
				if operation == "get" {
					source = strings.Replace(source, "(output/body)", "(output/view)", 1)
				}
				path := filepath.Join(root, "source", "records.dql")
				require.NoError(t, os.WriteFile(path, []byte(source), 0600))
				dest := filepath.Join(root, "records")
				require.NoError(t, os.Mkdir(dest, 0700))
				manifest := filepath.Join(dest, ".datly-gen.json")
				if legacy != "absent" {
					data := `{"version":999,"owner":"Wrong","files":["hooks.go"]}`
					if legacy == "malformed" {
						data = "{broken"
					}
					require.NoError(t, os.WriteFile(manifest, []byte(data), 0600))
				}
				hook := "package records\nimport \"context\"\nconst ApplicationOwned = true\nvar ApplicationInitCalls int\nfunc(*RecordsInput)Init(context.Context)error{ApplicationInitCalls++;return nil}\n"
				require.NoError(t, os.WriteFile(filepath.Join(dest, "hooks.go"), []byte(hook), 0600))
				run := func() {
					t.Helper()
					var out, diagnostic bytes.Buffer
					require.Equal(t, 0, generationCommand(context.Background(), []string{"transcribe", operation, "-dir", root, module + "/source"}, &out, &diagnostic), diagnostic.String())
					require.NoError(t, filepath.WalkDir(root, func(path string, entry os.DirEntry, err error) error {
						if err != nil {
							return err
						}
						require.NotEqual(t, ".datly-gen.json", entry.Name())
						return nil
					}))
					data, err := os.ReadFile(filepath.Join(dest, "hooks.go"))
					require.NoError(t, err)
					require.Equal(t, hook, string(data))
				}
				run()
				run()
				shape := filepath.Join(dest, "views.go")
				data, err := os.ReadFile(shape)
				require.NoError(t, err)
				require.NoError(t, os.WriteFile(shape, []byte(strings.Replace(string(data), `sqlx:"NAME"`, `sqlx:"NAME" custom:"edited"`, 1)+"\n// direct edit\n"), 0600))
				source = strings.Replace(source, "CAST(r.NAME AS string)", "CAST(r.NAME AS *string)", 1)
				require.NoError(t, os.WriteFile(path, []byte(source), 0600))
				run()
				data, err = os.ReadFile(shape)
				require.NoError(t, err)
				require.Regexp(t, `Name\s+\*string`, string(data))
				require.NotContains(t, string(data), "direct edit")
				require.NotContains(t, string(data), `custom:"edited"`)
				if legacy == "absent" {
					runtime := strings.NewReplacer("HOLDER", "RecordsComponent", "OPERATION", operation, "METHOD", strings.ToUpper(operation)).Replace(manifestfree.RuntimeSource)
					require.NoError(t, os.WriteFile(filepath.Join(dest, "runtime_test.go"), []byte(runtime), 0644))
					cmd := exec.Command("go", "test", "-mod=mod", "-count=1", "./...")
					cmd.Dir = root
					out, err := cmd.CombinedOutput()
					require.NoError(t, err, "%s", out)
				}

			})
		}
	}
}
