package dependency

import (
	"context"
	"github.com/stretchr/testify/require"
	xmodule "github.com/viant/x/module"
	"os"
	"path/filepath"
	"testing"
)

func TestSnapshotRetainsConsumedModelAndBytes(t *testing.T) {
	dir := t.TempDir()
	write := func(name, text string) {
		require.NoError(t, os.MkdirAll(filepath.Dir(filepath.Join(dir, name)), 0755))
		require.NoError(t, os.WriteFile(filepath.Join(dir, name), []byte(text), 0644))
	}
	write("go.mod", "module example.com/snapshot\n\ngo 1.24\n")
	original := "package one\ntype Record struct { ID int }\n"
	write("one/record.go", original)
	write("one/excluded.go", "not Go source")
	write("two/record.go", "package two\ntype Other struct { ID int }\n")
	workspace, err := (xmodule.LocalWorkspace{BaseDir: dir}).Resolve(context.Background())
	require.NoError(t, err)
	snapshot := &Snapshot{Select: func(context.Context, string, string) ([]string, error) { return []string{"record.go"}, nil }}
	first, err := loadSnapshotPackage(context.Background(), workspace, "example.com/snapshot/one", snapshot)
	require.NoError(t, err)
	require.Len(t, snapshot.Files, 2)
	require.Equal(t, []byte(original), snapshot.Files[1].Bytes)
	write("one/record.go", "not Go anymore")
	second, err := loadSnapshotPackage(context.Background(), workspace, "example.com/snapshot/one", snapshot)
	require.NoError(t, err)
	require.Same(t, first, second, "reuse the loaded AST; no second parse")
	require.Equal(t, []byte(original), snapshot.Files[1].Bytes)
	_, err = loadSnapshotPackage(context.Background(), workspace, "example.com/snapshot/two", snapshot)
	require.NoError(t, err)
	require.Len(t, snapshot.Files, 3, "shared module source is sealed once")
}
