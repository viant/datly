package transcribe

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
	xmodule "github.com/viant/x/module"
)

func TestDiscoveryInventoryMatchesWorkspaceSelection(t *testing.T) {
	root := t.TempDir()
	write := func(name, content string) {
		require.NoError(t, os.MkdirAll(filepath.Dir(filepath.Join(root, name)), 0755))
		require.NoError(t, os.WriteFile(filepath.Join(root, name), []byte(content), 0600))
	}
	write("go.mod", "module example.com/inventory\n\ngo 1.25\n")
	write(".goignore", "archived\n")
	for _, dir := range []string{"source", "source/nested", "other", "archived", "vendor", ".hidden", "_hidden", "testdata", "nested-module"} {
		write(dir+"/rule.dql", "SELECT 1")
	}
	write("nested-module/go.mod", "module example.com/nested\n")
	require.NoError(t, os.Symlink(filepath.Join(root, "source"), filepath.Join(root, "alias")))
	workspace, err := (xmodule.LocalWorkspace{BaseDir: root}).Resolve(t.Context())
	require.NoError(t, err)
	inventory, err := discoverFiles(t.Context(), workspace, []string{"..."}, nil)
	require.NoError(t, err)
	for _, test := range []struct {
		name             string
		include, exclude []string
	}{
		{"all", []string{"..."}, nil},
		{"recursive", []string{"example.com/inventory/source/..."}, nil},
		{"exact", []string{"example.com/inventory/source"}, nil},
		{"glob", []string{"example.com/inventory/s*"}, nil},
		{"overlap", []string{"example.com/inventory/source", "example.com/inventory/..."}, nil},
		{"exclude", []string{"..."}, []string{"example.com/inventory/source/..."}},
		{"glob exclude", []string{"..."}, []string{"example.com/inventory/*"}},
		{"empty", nil, nil},
		{"blank", []string{""}, nil},
		{"invalid include", []string{"["}, nil},
		{"invalid exclude", []string{"..."}, []string{"["}},
	} {
		t.Run(test.name, func(t *testing.T) {
			var expected []xmodule.File
			wantErr := workspace.Walk(t.Context(), test.include, test.exclude, func(file xmodule.File) error { expected = append(expected, file); return nil })
			actual, err := inventory.selectFiles(t.Context(), test.include, test.exclude)
			if wantErr != nil {
				require.EqualError(t, err, wantErr.Error())
				return
			}
			require.NoError(t, err)
			require.Equal(t, expected, actual)
		})
	}
	canceled, cancel := context.WithCancel(t.Context())
	cancel()
	_, err = discoveryFiles(nil).selectFiles(canceled, []string{"..."}, nil)
	require.ErrorIs(t, err, context.Canceled)
}

func TestDiscoveryInventoryRetainsBuildSelectionScope(t *testing.T) {
	root := t.TempDir()
	require.NoError(t, os.WriteFile(filepath.Join(root, "go.mod"), []byte("module example.com/inventory\n"), 0600))
	require.NoError(t, os.Mkdir(filepath.Join(root, "source"), 0755))
	require.NoError(t, os.WriteFile(filepath.Join(root, "source/selected.go"), []byte("package source\ntype Selected struct { ID int }"), 0600))
	require.NoError(t, os.WriteFile(filepath.Join(root, "source/excluded.go"), []byte("not selected Go source"), 0600))
	require.NoError(t, os.WriteFile(filepath.Join(root, "source/rule.dql"), []byte(`#package('example.com/inventory/api')
#setting($_ = $input_type('Input'))
#setting($_ = $output_type('Output'))
#setting($_ = $route('/records','GET'))
#define($_ = $Data<[]*Row>(output/view))
SELECT r.*, type(r,'Row'), CAST(r.ID AS int) FROM (SELECT 1 AS ID) r`), 0600))
	module := &xmodule.BuildModule{Path: "example.com/inventory", Dir: root, Main: true}
	workspace := (&xmodule.BuildSelection{Packages: []xmodule.BuildPackage{
		{ImportPath: "example.com/inventory/source", Dir: filepath.Join(root, "source"), Name: "source", GoFiles: []string{"selected.go"}, Module: module},
		// A stale unselected package must not become a discovery dependency.
		{ImportPath: "example.com/inventory/unselected", Dir: filepath.Join(root, "missing"), Name: "unselected", Module: module},
	}}).Workspace()
	files, err := discoverFiles(t.Context(), workspace, []string{"example.com/inventory/source"}, nil)
	require.NoError(t, err)
	require.Len(t, files, 2)
	project, err := (&Discovery{BaseDir: root, Workspace: workspace, Include: []string{"example.com/inventory/source"}}).Compile(t.Context())
	require.NoError(t, err)
	require.Len(t, project.Components, 1)
}
