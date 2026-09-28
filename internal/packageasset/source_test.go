package packageasset

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestSourceResourcesMultipleNamespacesAndFolders(t *testing.T) {
	dir := t.TempDir()
	for _, name := range []string{"sql/one.sql", "sql/two.sql", "static/file.txt", "skills/demo/SKILL.md", "skills/demo/_private.txt"} {
		path := filepath.Join(dir, name)
		require.NoError(t, os.MkdirAll(filepath.Dir(path), 0755))
		require.NoError(t, os.WriteFile(path, []byte(name), 0644))
	}
	source := `package sample
import "embed"
const OneDatlyResourceNamespace="one"
//go:embed sql/*.sql static skills
var OneDatlyResources embed.FS
const TwoDatlyResourceNamespace="two"
//go:embed sql/two.sql
var TwoDatlyResources embed.FS
`
	require.NoError(t, os.WriteFile(filepath.Join(dir, "resources.go"), []byte(source), 0644))
	resources, err := DiscoverSourceResources(dir)
	require.NoError(t, err)
	require.Len(t, resources, 2)
	require.Equal(t, "one", resources[0].Namespace)
	require.Equal(t, []string{"skills/demo/SKILL.md", "sql/one.sql", "sql/two.sql", "static/file.txt"}, resources[0].Files)
	require.Equal(t, []string{"sql/two.sql"}, resources[1].Files)
}

func TestSourceResourcesRejectDuplicateNamespace(t *testing.T) {
	dir := t.TempDir()
	source := `package sample
import "embed"
const OneDatlyResourceNamespace="same"
const TwoDatlyResourceNamespace="same"
//go:embed one.sql
var OneDatlyResources embed.FS
//go:embed two.sql
var TwoDatlyResources embed.FS
`
	require.NoError(t, os.WriteFile(filepath.Join(dir, "resources.go"), []byte(source), 0644))
	_, err := DiscoverSourceResources(dir)
	require.ErrorContains(t, err, "duplicate resource namespace")
}
