package generate

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestImportValidationTraversal(t *testing.T) {
	for _, test := range []struct {
		name, directory, source string
		nested, link, fail      bool
	}{
		{"authored intermediary cycle", "bridge", "package bridge\nimport _ \"example.com/walk/api\"", false, false, true},
		{"independent package", "bridge", "package bridge", false, false, false},
		{"malformed authored package", "bridge", "package", false, false, true},
		{"nested module", "nested", "package", true, false, false},
		{"hidden directory", ".hidden", "package", false, false, false},
		{"vendor", "vendor", "package", false, false, false},
		{"directory symlink", "linked", "package", false, true, false},
	} {
		t.Run(test.name, func(t *testing.T) {
			root := t.TempDir()
			require.NoError(t, os.WriteFile(filepath.Join(root, "go.mod"), []byte("module example.com/walk\n"), 0600))
			require.NoError(t, os.Mkdir(filepath.Join(root, "api"), 0755))
			directory := filepath.Join(root, test.directory)
			if test.link {
				directory = t.TempDir()
				require.NoError(t, os.Symlink(directory, filepath.Join(root, test.directory)))
			} else {
				require.NoError(t, os.MkdirAll(directory, 0755))
			}
			require.NoError(t, os.WriteFile(filepath.Join(directory, "source.go"), []byte(test.source), 0600))
			if test.nested {
				require.NoError(t, os.WriteFile(filepath.Join(directory, "go.mod"), []byte("module example.com/nested\n"), 0600))
			}
			proposed := EmittedFile{Path: filepath.Join(root, "api/generated.go"), Content: "package api\nimport _ \"example.com/walk/bridge\""}
			set := &packageSet{plans: []*Plan{{ProjectRoot: root, Package: "example.com/walk/api"}}, dirs: []string{filepath.Join(root, "api")}, files: [][]EmittedFile{{proposed}}}
			err := set.validateImports()
			if test.fail {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
			}
		})
	}
}

func TestImportValidationUsesProjectedForest(t *testing.T) {
	for _, projectedCycle := range []bool{false, true} {
		t.Run(map[bool]string{false: "removed cycle", true: "introduced cycle"}[projectedCycle], func(t *testing.T) {
			root, stage := t.TempDir(), t.TempDir()
			require.NoError(t, os.WriteFile(filepath.Join(root, "go.mod"), []byte("module example.com/walk\n"), 0600))
			for _, dir := range []string{"api", "bridge"} {
				require.NoError(t, os.Mkdir(filepath.Join(root, dir), 0755))
			}
			plain := "package bridge\n"
			cycle := "package bridge\nimport _ \"example.com/walk/api\"\n"
			physical, projected := cycle, plain
			if projectedCycle {
				physical, projected = plain, cycle
			}
			require.NoError(t, os.WriteFile(filepath.Join(root, "bridge/source.go"), []byte(physical), 0600))
			require.NoError(t, os.WriteFile(filepath.Join(stage, "source.go"), []byte(projected), 0600))
			set := &packageSet{
				plans:   []*Plan{{ProjectRoot: root, Package: "example.com/walk/api"}},
				dirs:    []string{filepath.Join(root, "api")},
				files:   [][]EmittedFile{{{Path: filepath.Join(root, "api/generated.go"), Content: "package api\nimport _ \"example.com/walk/bridge\""}}},
				forests: []*scaffoldForest{{target: filepath.Join(root, "bridge"), stage: stage}},
			}
			err := set.validateImports()
			if projectedCycle {
				require.ErrorContains(t, err, "generation import cycle")
			} else {
				require.NoError(t, err)
			}
		})
	}
}
