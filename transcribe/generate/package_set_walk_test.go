package generate

import (
	"fmt"
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

// Validation must refresh the whole graph between passes, even when an edit
// preserves size and timestamp or a package appears after the first pass.
func TestImportValidationRefreshesProject(t *testing.T) {
	root := t.TempDir()
	write := func(name, source string) {
		t.Helper()
		require.NoError(t, os.MkdirAll(filepath.Dir(filepath.Join(root, name)), 0755))
		require.NoError(t, os.WriteFile(filepath.Join(root, name), []byte(source), 0600))
	}
	write("go.mod", "module example.com/walk\n")
	for i := 0; i < 80; i++ {
		write(fmt.Sprintf("branch%02d/leaf/source.go", i), "package leaf\n")
	}
	plain := "package bridge\nimport _ \"example.com/walk/end\"\n"
	cycle := "package bridge\nimport _ \"example.com/walk/api\"\n"
	write("bridge/source.go", plain)
	set := &packageSet{plans: []*Plan{{ProjectRoot: root, Package: "example.com/walk/api"}}, dirs: []string{filepath.Join(root, "api")}, files: [][]EmittedFile{{{Path: filepath.Join(root, "api/generated.go"), Content: "package api\nimport _ \"example.com/walk/bridge\"\n"}}}}
	require.NoError(t, set.validateImports())
	info, err := os.Stat(filepath.Join(root, "bridge/source.go"))
	require.NoError(t, err)
	write("bridge/source.go", cycle)
	require.NoError(t, os.Chtimes(filepath.Join(root, "bridge/source.go"), info.ModTime(), info.ModTime()))
	require.ErrorContains(t, set.validateImports(), "generation import cycle")
	write("bridge/source.go", plain)
	write("end/source.go", "package end\nimport _ \"example.com/walk/api\"\n")
	require.ErrorContains(t, set.validateImports(), "generation import cycle")
	require.NoError(t, os.Remove(filepath.Join(root, "end/source.go")))
	require.NoError(t, set.validateImports())
	write("end/source.go", "package end\nfunc broken(\n")
	require.Error(t, set.validateImports())
	write("end/go.mod", "module example.com/nested\n")
	require.NoError(t, set.validateImports())
	require.NoError(t, os.Remove(filepath.Join(root, "end/go.mod")))
	require.Error(t, set.validateImports())
}

func TestImportValidationModuleBoundaryEntries(t *testing.T) {
	for _, test := range []struct {
		name                      string
		link, dangling, directory bool
		wantError                 bool
	}{
		{name: "module file"},
		{name: "module directory", directory: true},
		{name: "module symlink", link: true},
		{name: "dangling module symlink", link: true, dangling: true, wantError: true},
	} {
		t.Run(test.name, func(t *testing.T) {
			root := t.TempDir()
			require.NoError(t, os.WriteFile(filepath.Join(root, "go.mod"), []byte("module example.com/walk\n"), 0600))
			nested := filepath.Join(root, "nested")
			require.NoError(t, os.Mkdir(nested, 0755))
			require.NoError(t, os.WriteFile(filepath.Join(nested, "bad.go"), []byte("package broken\nfunc broken(\n"), 0600))
			boundary := filepath.Join(nested, "go.mod")
			switch {
			case test.directory:
				require.NoError(t, os.Mkdir(boundary, 0755))
			case test.link:
				target := filepath.Join(t.TempDir(), "module")
				if !test.dangling {
					require.NoError(t, os.WriteFile(target, []byte("module example.com/nested\n"), 0600))
				}
				require.NoError(t, os.Symlink(target, boundary))
			default:
				require.NoError(t, os.WriteFile(boundary, []byte("module example.com/nested\n"), 0600))
			}
			set := &packageSet{plans: []*Plan{{ProjectRoot: root, Package: "example.com/walk/api"}}, dirs: []string{filepath.Join(root, "api")}, files: [][]EmittedFile{{{Path: filepath.Join(root, "api/generated.go"), Content: "package api\n"}}}}
			err := set.validateImports()
			if test.wantError {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
			}
		})
	}
}

func TestImportValidationKeepsTraversalErrorOrder(t *testing.T) {
	root := t.TempDir()
	require.NoError(t, os.WriteFile(filepath.Join(root, "go.mod"), []byte("module example.com/walk\n"), 0600))
	write := func(name, source string) {
		t.Helper()
		dir := filepath.Join(root, name)
		require.NoError(t, os.MkdirAll(dir, 0755))
		require.NoError(t, os.WriteFile(filepath.Join(dir, "source.go"), []byte(source), 0600))
	}
	write("a/deep/first", "package first\nfunc broken(\n")
	set := &packageSet{plans: []*Plan{{ProjectRoot: root, Package: "example.com/walk/api"}}, dirs: []string{filepath.Join(root, "api")}, files: [][]EmittedFile{{{Path: filepath.Join(root, "api/generated.go"), Content: "package api\n"}}}}
	first := set.validateImports()
	require.Error(t, first)
	// Later errors can complete before the first deep subtree; directory order
	// must still select the same diagnostic, and every pass must join its workers.
	for i := 0; i < 30; i++ {
		write(fmt.Sprintf("z%02d", i), "package\n")
	}
	for i := 0; i < 10; i++ {
		require.EqualError(t, set.validateImports(), first.Error())
	}
}
