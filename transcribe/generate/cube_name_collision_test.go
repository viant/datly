package generate

import (
	"io/fs"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/viant/datly/internal/testharness"
	"github.com/viant/datly/spec"
)

func TestCubeWireCollisionDoesNotPublish(t *testing.T) {
	root := t.TempDir()
	testharness.WriteGeneratedGoMod(t, root)
	destination := filepath.Join(root, "reporting")
	component := cubeFixture()
	_, err := New(Input{Component: component, TargetPackage: "example.com/generated/reporting"}).Generate(destination)
	require.NoError(t, err)
	snapshot := func() map[string]string {
		t.Helper()
		files := map[string]string{}
		require.NoError(t, filepath.WalkDir(root, func(path string, entry fs.DirEntry, err error) error {
			if err != nil {
				return err
			}
			if entry.IsDir() {
				return nil
			}
			content, err := os.ReadFile(path)
			if err != nil {
				return err
			}
			files[path] = string(content)
			return nil
		}))
		return files
	}
	before := snapshot()
	component.RootView.Columns[0].Name = "AccountIDs"
	component.RootView.Columns = append(component.RootView.Columns, &spec.Column{
		Name: "Account_Ids", Source: "other_id", Type: spec.TypeRef{Name: "int"}, Groupable: component.RootView.Groupable,
	})
	component.RootView.Source.SQL = "SELECT account_id,other_id,SUM(amount) AS amount FROM spend GROUP BY account_id,other_id"
	_, err = New(Input{Component: component, TargetPackage: "example.com/generated/reporting"}).Generate(destination)
	require.ErrorContains(t, err, "AccountIDs")
	require.ErrorContains(t, err, "Account_Ids")
	require.ErrorContains(t, err, `wire name "accountIds"`)
	require.Equal(t, before, snapshot(), "invalid cube metadata changed files or ownership manifests")
}
