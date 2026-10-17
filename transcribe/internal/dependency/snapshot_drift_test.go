package dependency

import (
	"context"
	"github.com/stretchr/testify/require"
	xmodule "github.com/viant/x/module"
	"os"
	"path/filepath"
	"testing"
)

func TestSnapshotRejectsModuleDriftWithoutPublishingPackage(t *testing.T) {
	dir := t.TempDir()
	write := func(name, text string) {
		require.NoError(t, os.MkdirAll(filepath.Dir(filepath.Join(dir, name)), 0755))
		require.NoError(t, os.WriteFile(filepath.Join(dir, name), []byte(text), 0644))
	}
	original := "module example.com/drift\n\ngo 1.24\n"
	write("go.mod", original)
	write("one/record.go", "package one\ntype Record struct { ID int }\n")
	write("two/record.go", "package two\ntype Record struct { ID int }\n")
	workspace, err := (xmodule.LocalWorkspace{BaseDir: dir}).Resolve(context.Background())
	require.NoError(t, err)
	snapshot := &Snapshot{Select: func(context.Context, string, string) ([]string, error) { return []string{"record.go"}, nil }}
	_, err = loadSnapshotPackage(context.Background(), workspace, "example.com/drift/one", snapshot)
	require.NoError(t, err)
	write("go.mod", original+"// changed after first dependency was consumed\n")
	_, err = loadSnapshotPackage(context.Background(), workspace, "example.com/drift/two", snapshot)
	require.ErrorContains(t, err, "dependency source changed during compilation")
	require.Nil(t, snapshot.Packages["example.com/drift/two"])
	require.Len(t, snapshot.Files, 2)
}
