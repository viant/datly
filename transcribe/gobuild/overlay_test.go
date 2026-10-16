package gobuild

import (
	"encoding/json"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestValidateMergesInheritedOverlay(t *testing.T) {
	root := t.TempDir()
	write := func(name, content string) {
		t.Helper()
		file := filepath.Join(root, name)
		require.NoError(t, os.MkdirAll(filepath.Dir(file), 0700))
		require.NoError(t, os.WriteFile(file, []byte(content), 0600))
	}
	write("go.mod", "module example.com/overlay\ngo 1.25.0\n")
	write("dependency/value.go", "package dependency\nconst Old = 1\n")
	write("dependency/deleted.go", "package dependency\nTHIS IS INVALID\n")
	write("backing/value.go", "package dependency\nconst Value = 42\n")
	write("backing/wrong.go", "package endpoint\nvar Value = Missing\n")
	write("endpoint/value.go", "package endpoint\nvar Value = Missing\n")
	write("endpoint/deleted.go", "package endpoint\nTHIS IS INVALID\n")
	write("backing/deleted.go", "package endpoint\nTHIS IS INVALID\n")
	replacements := map[string]string{
		"dependency/value.go":   "backing/value.go",
		"dependency/deleted.go": "",
		"endpoint/value.go":     "backing/wrong.go",
		"endpoint/deleted.go":   "backing/deleted.go",
	}
	data, err := json.Marshal(struct{ Replace map[string]string }{replacements})
	require.NoError(t, err)
	write("configuration/overlay with spaces.json", string(data))
	// Context.Env overrides inherited GOFLAGS. Paths in the overlay are relative
	// to the Go process directory, not the overlay's configuration directory.
	t.Setenv("GOFLAGS", "-overlay=does-not-exist.json")
	build := &Context{Dir: root, Env: []string{"GOWORK=off", "GOFLAGS=-overlay=ignored.json '-overlay=configuration/overlay with spaces.json'"}}
	files, err := build.ValidateFiles("example.com/overlay/endpoint", map[string][]byte{
		"endpoint/value.go":   []byte("package endpoint\nimport \"example.com/overlay/dependency\"\nvar Value = dependency.Value\n"),
		"endpoint/deleted.go": nil,
	})
	require.NoError(t, err)
	require.Equal(t, []string{"value.go"}, files)
	original, err := os.ReadFile(filepath.Join(root, "endpoint/value.go"))
	require.NoError(t, err)
	require.Contains(t, string(original), "Missing")
}

func TestInheritedOverlayErrorsAndDisable(t *testing.T) {
	root := t.TempDir()
	require.NoError(t, os.WriteFile(filepath.Join(root, "malformed.json"), []byte("{"), 0600))
	for _, test := range []struct{ flags, error string }{
		{"-overlay=missing.json", "read inherited Go overlay"},
		{"-overlay=malformed.json", "decode inherited Go overlay"},
	} {
		t.Run(test.flags, func(t *testing.T) {
			build := &Context{Dir: root, Env: []string{"GOWORK=off", "GOFLAGS=" + test.flags}}
			_, err := build.inheritedOverlay()
			require.ErrorContains(t, err, test.error)
		})
	}
	build := &Context{Dir: root, Env: []string{"GOWORK=off", "GOFLAGS=-overlay=missing.json -overlay="}}
	replace, err := build.inheritedOverlay()
	require.NoError(t, err)
	require.Empty(t, replace)
}

func TestSplitGoFlags(t *testing.T) {
	flags, err := splitGoFlags(" -mod=readonly '-overlay=path with spaces.json' \"-tags=a b\" ")
	require.NoError(t, err)
	require.Equal(t, []string{"-mod=readonly", "-overlay=path with spaces.json", "-tags=a b"}, flags)
	_, err = splitGoFlags("'-overlay=unfinished")
	require.Error(t, err)
}
