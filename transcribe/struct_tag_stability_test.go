package transcribe

import (
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"strconv"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/viant/datly/internal/testharness"
)

// Separate processes exercise clean generation and regeneration without sharing
// the compiler's in-memory package state or the process's map iteration seed.
func TestStructTagGenerationStability(t *testing.T) {
	const workerRoot = "DATLY_TAG_STABILITY_ROOT"
	if root := os.Getenv(workerRoot); root != "" {
		const text = `#package('pkg/constraints')
#setting($_ = $route('/constraints/reset', 'POST'))
#setting($_ = $const('MdpAggregationConstraintsTable','CI_FEATURE_AGGREGATION_CONSTRAINT'))
#define($_ = $ConstraintType<int>(query/type))
$dml.Execute("UPDATE ` + "`${MdpAggregationConstraintsTable}`" + ` SET audience_ids = '' WHERE type = ?", $Input.ConstraintType)`
		_, err := (Generator{Operation: "post"}).Generate(t.Context(), GenerationRequest{
			Destination: root,
			Source:      &Source{Name: "ResetConstraints", Scope: "github.com/viant/datly/tagstability/pkg/constraints", Connector: "main", Text: text},
		})
		require.NoError(t, err)
		return
	}
	executable, err := os.Executable()
	require.NoError(t, err)
	var baseline map[string]string
	for range 2 {
		root := t.TempDir()
		(testharness.GeneratedModule{Path: "github.com/viant/datly/tagstability"}).Write(t, root)
		for range 3 {
			cmd := exec.CommandContext(t.Context(), executable, "-test.run=^TestStructTagGenerationStability$", "-test.count=1")
			cmd.Env = append(os.Environ(), workerRoot+"="+root)
			out, err := cmd.CombinedOutput()
			require.NoError(t, err, "%s", out)
			current := snapshotTagArtifacts(t, root)
			if baseline == nil {
				baseline = current
			} else {
				require.Equal(t, baseline, current, "all artifacts must match across clean generation and regeneration")
			}
		}
		// Handwritten contracts and their original tag ordering are not rewritten.
		const handwritten = "package constraints\ntype Authored struct { Value string `z:\"\" json:\"value\"` }\n"
		path := filepath.Join(root, "pkg/constraints/authored.go")
		require.NoError(t, os.WriteFile(path, []byte(handwritten), 0600))
		cmd := exec.CommandContext(t.Context(), executable, "-test.run=^TestStructTagGenerationStability$", "-test.count=1")
		cmd.Env = append(os.Environ(), workerRoot+"="+root)
		out, err := cmd.CombinedOutput()
		require.NoError(t, err, "%s", out)
		body, err := os.ReadFile(path)
		require.NoError(t, err)
		require.Equal(t, handwritten, string(body))
	}
}

func snapshotTagArtifacts(t *testing.T, root string) map[string]string {
	t.Helper()
	files := map[string]string{}
	found := false
	require.NoError(t, filepath.WalkDir(filepath.Join(root, "pkg"), func(path string, entry os.DirEntry, err error) error {
		if err != nil || entry.IsDir() {
			return err
		}
		body, err := os.ReadFile(path)
		if err != nil {
			return err
		}
		relative, err := filepath.Rel(root, path)
		if err != nil {
			return err
		}
		files[relative] = string(body)
		if filepath.Ext(path) != ".go" {
			return nil
		}
		file, err := parser.ParseFile(token.NewFileSet(), path, body, 0)
		if err != nil {
			return err
		}
		ast.Inspect(file, func(node ast.Node) bool {
			field, ok := node.(*ast.Field)
			if !ok || field.Tag == nil || len(field.Names) != 1 || field.Names[0].Name != "MdpAggregationConstraintsTable" {
				return true
			}
			literal, err := strconv.Unquote(field.Tag.Value)
			require.NoError(t, err)
			tag := reflect.StructTag(literal)
			require.Equal(t, "true", tag.Get("internal"))
			require.Equal(t, "MdpAggregationConstraintsTable,kind=const,in=MdpAggregationConstraintsTable,dataType=string,value=CI_FEATURE_AGGREGATION_CONSTRAINT", tag.Get("parameter"))
			require.Equal(t, `internal:"true" parameter:"`+tag.Get("parameter")+`"`, literal)
			found = true
			return true
		})
		return nil
	}))
	require.True(t, found, "generated constant field must retain both tags")
	require.NotEmpty(t, files)
	return files
}
