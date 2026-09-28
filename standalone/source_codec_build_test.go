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

//go:embed testdata/codecapp
var codecApp embed.FS

func TestLinkedCodecsSourceFreeEagerAndIndexed(t *testing.T) {
	root := filepath.Join(t.TempDir(), "source")
	require.NoError(t, os.MkdirAll(root, 0700))
	files, err := fs.Sub(codecApp, "testdata/codecapp")
	require.NoError(t, err)
	require.NoError(t, os.CopyFS(root, files))
	(testharness.GeneratedModule{Path: "example.com/codecapp"}).Write(t, root)
	codec, err := os.ReadFile("../transcribe/testdata/linkedcodec/codec.go")
	require.NoError(t, err)
	require.NoError(t, os.MkdirAll(filepath.Join(root, "transforms"), 0700))
	require.NoError(t, os.WriteFile(filepath.Join(root, "transforms/codec.go"), codec, 0600))
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()
	result, err := (build.Service{}).SyncLinks(ctx, build.LinkRequest{Dir: root})
	require.NoError(t, err)
	require.Contains(t, result.Added, "example.com/codecapp/transforms")
	binary := filepath.Join(t.TempDir(), "codec-app")
	command := exec.CommandContext(ctx, "go", "build", "-mod=readonly", "-o", binary, ".")
	command.Dir, command.Env = root, append(os.Environ(), "GOWORK=off")
	output, err := command.CombinedOutput()
	require.NoError(t, err, string(output))
	for _, mode := range []string{"indexed", "eager"} {
		t.Run(mode, func(t *testing.T) {
			if mode == "eager" {
				require.NoError(t, os.Rename(root, root+"-unavailable"))
			}
			command := exec.CommandContext(ctx, binary, root, mode, filepath.Join(t.TempDir(), "records.db"))
			command.Dir = t.TempDir()
			output, err := command.CombinedOutput()
			require.NoError(t, err, string(output))
			require.Contains(t, string(output), "parameter, child and summary codecs applied through HTTP and MCP")
		})
	}
}
