package standalone

import (
	"context"
	"embed"
	"io/fs"
	"os"
	"os/exec"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/viant/datly/internal/testharness"
	"github.com/viant/datly/project/build"
)

//go:embed testdata/predicateapp
var predicateApp embed.FS

func TestLinkedPredicatesSourceFreeEagerAndIndexed(t *testing.T) {
	root := filepath.Join(t.TempDir(), "source")
	require.NoError(t, os.MkdirAll(root, 0700))
	files, err := fs.Sub(predicateApp, "testdata/predicateapp")
	require.NoError(t, err)
	require.NoError(t, os.CopyFS(root, files))
	(testharness.GeneratedModule{Path: "example.com/predicateapp"}).Write(t, root)
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()
	result, err := (build.Service{}).SyncLinks(ctx, build.LinkRequest{Dir: root})
	require.NoError(t, err)
	require.Contains(t, result.Added, "example.com/predicateapp/security")
	link, err := os.ReadFile(filepath.Join(root, "internal/dependencylink/link.go"))
	require.NoError(t, err)
	require.NotContains(t, string(link), "Register")
	binary := filepath.Join(t.TempDir(), "predicate-app")
	command := exec.CommandContext(ctx, "go", "build", "-mod=mod", "-o", binary, ".")
	command.Dir = root
	command.Env = append(os.Environ(), "GOWORK=off")
	output, err := command.CombinedOutput()
	require.NoError(t, err, string(output))
	for _, mode := range []string{"indexed", "eager"} {
		t.Run(mode, func(t *testing.T) {
			if mode == "eager" {
				require.NoError(t, os.Rename(root, root+"-unavailable"))
				_, err := os.Stat(root)
				require.True(t, os.IsNotExist(err))
			}
			command := exec.CommandContext(ctx, binary, root, mode, filepath.Join(t.TempDir(), "records.db"))
			command.Dir = t.TempDir()
			output, err := command.CombinedOutput()
			require.NoError(t, err, string(output))
			require.Contains(t, string(output), "predicate applied through HTTP and MCP")
		})
	}
}
