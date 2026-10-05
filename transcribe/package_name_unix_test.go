//go:build unix

package transcribe

import (
	"context"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
	"golang.org/x/sys/unix"
)

func TestGeneratorExistingPrimaryPackageRejectsNonregular(t *testing.T) {
	root, source := primaryPackageFixture(t)
	writePrimaryPackageFile(t, root, "a.go", "package campaign\n")
	require.NoError(t, unix.Mkfifo(filepath.Join(root, primaryPackageDirectory, "b.go"), 0600))
	before := primaryPackageSnapshot(t, root)
	_, err := (Generator{Operation: "get"}).Generate(context.Background(), GenerationRequest{Source: source, Destination: root})
	require.ErrorContains(t, err, "b.go\" is not regular")
	require.Equal(t, before, primaryPackageSnapshot(t, root))
}
