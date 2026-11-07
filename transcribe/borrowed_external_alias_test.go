package transcribe

import (
	"archive/zip"
	"bytes"
	"os"
	"os/exec"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/viant/datly/transcribe/gobuild"
	"github.com/viant/datly/transcribe/internal/dependency"
	"github.com/viant/datly/typecatalog"
)

// A downloaded module intentionally has no local replacement. Reflection cannot
// recover its source alias, so fresh proofs must retain Go-selected declarations.
func TestBorrowedFreshExternalSourceAlias(t *testing.T) {
	root := t.TempDir()
	proxy := filepath.Join(t.TempDir(), "proxy")
	cache := filepath.Join(t.TempDir(), "modcache")
	t.Cleanup(func() {
		_ = filepath.Walk(cache, func(path string, info os.FileInfo, err error) error {
			if err == nil && info.IsDir() {
				_ = os.Chmod(path, 0700)
			}
			return nil
		})
	})
	const module = "example.com/borrowedforeign"
	const version = "v1.0.0"
	mod := []byte("module " + module + "\n\ngo 1.25\n")
	source := []byte("//go:build borrowed_alias\n\npackage foreign\ntype Canonical struct { Allowed bool }\ntype Output = Canonical\n")
	directory := filepath.Join(proxy, module, "@v")
	require.NoError(t, os.MkdirAll(directory, 0700))
	require.NoError(t, os.WriteFile(filepath.Join(directory, version+".mod"), mod, 0600))
	require.NoError(t, os.WriteFile(filepath.Join(directory, version+".info"), []byte(`{"Version":"v1.0.0","Time":"2026-01-01T00:00:00Z"}`), 0600))
	require.NoError(t, os.WriteFile(filepath.Join(directory, "list"), []byte(version+"\n"), 0600))
	var archive bytes.Buffer
	writer := zip.NewWriter(&archive)
	for name, data := range map[string][]byte{"go.mod": mod, "types.go": source, "base.go": []byte("package foreign\ntype Base struct{}\n")} {
		entry, err := writer.Create(module + "@" + version + "/" + name)
		require.NoError(t, err)
		_, err = entry.Write(data)
		require.NoError(t, err)
	}
	require.NoError(t, writer.Close())
	require.NoError(t, os.WriteFile(filepath.Join(directory, version+".zip"), archive.Bytes(), 0600))
	require.NoError(t, os.WriteFile(filepath.Join(root, "go.mod"), []byte("module example.com/borrower\n\ngo 1.25\nrequire "+module+" "+version+"\n"), 0600))
	environment := []string{"GOWORK=off", "GOPROXY=file://" + proxy, "GOSUMDB=off", "GOMODCACHE=" + cache, "GOFLAGS=-mod=readonly"}
	command := exec.CommandContext(t.Context(), "go", "mod", "download", module)
	command.Dir = root
	command.Env = append(os.Environ(), environment...)
	out, err := command.CombinedOutput()
	require.NoError(t, err, string(out))
	text := "#package('example.com/borrower')\n#import('foreign','" + module + "')\n#define($_ = $Config<*foreign.Output>(param/Config))\n"
	s := &Source{Scope: "example.com/borrower", Path: filepath.Join(root, "probe.dql"), Text: text, GoBuild: &gobuild.Context{Dir: root, Env: environment, Tags: "borrowed_alias"}}
	require.NoError(t, os.WriteFile(s.Path, []byte(text), 0600))
	s.Text = text + "#setting($_ = $route('/probe','GET'))\nSELECT 1 AS id"
	ordinary, err := (&Discovery{BaseDir: root, GoBuild: s.GoBuild}).CompileSource(t.Context(), s)
	require.NoError(t, err)
	_, ordinaryFound, err := ordinary.Source.Types.Resolve(typecatalog.TranscribeAuthority, module+".Output")
	require.NoError(t, err)
	require.False(t, ordinaryFound, "ordinary discovery without borrowed source snapshot remains unchanged")
	s.dependencySnapshot = &dependency.Snapshot{}
	result, err := (&Discovery{BaseDir: root, GoBuild: s.GoBuild}).CompileSource(t.Context(), s)
	require.NoError(t, err)
	_, initialFound, err := result.Source.Types.Resolve(typecatalog.TranscribeAuthority, module+".Output")
	require.NoError(t, err)
	require.True(t, initialFound, "initial borrowed discovery must retain external alias before plan generation")
	(&Discovery{BaseDir: root}).prepareDependencySnapshot(t.Context(), s)
	catalog, err := freshBorrowedForeignTypes(t.Context(), s, text)
	require.NoError(t, err)
	descriptor, found, err := catalog.Resolve(typecatalog.TranscribeAuthority, module+".Output")
	require.NoError(t, err)
	require.True(t, found, "explicit downloaded alias lost from fresh proof catalog")
	require.NotNil(t, descriptor)
	require.NotNil(t, s.dependencySnapshot.Packages[module])
	var sealed bool
	for _, file := range s.dependencySnapshot.Files {
		if filepath.Base(file.Path) == "types.go" {
			require.Equal(t, source, file.Bytes)
			sealed = true
		}
	}
	require.True(t, sealed, "Go-selected alias source must participate in borrowed proof sealing")
	s.dependencySnapshot = &dependency.Snapshot{}
	s.GoBuild.Tags = ""
	(&Discovery{BaseDir: root}).prepareDependencySnapshot(t.Context(), s)
	catalog, err = freshBorrowedForeignTypes(t.Context(), s, text)
	require.NoError(t, err)
	_, found, err = catalog.Resolve(typecatalog.TranscribeAuthority, module+".Output")
	require.NoError(t, err)
	require.False(t, found, "build-excluded alias must not enter source authority")
}
