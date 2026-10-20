package transcribe

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestBorrowedOwnerSourceFilenameDiffersFromComponent(t *testing.T) {
	root, owner, child, _ := borrowedAuxiliaryOwnerFixture(t)
	originalPath := owner.Path
	owner.Path = filepath.Join(filepath.Dir(owner.Path), "source_file.dql")
	require.NoError(t, os.Rename(originalPath, owner.Path))
	child.Text = strings.ReplaceAll(child.Text, "'Owner.dql','Owner'", "'source_file.dql','source_file'")
	require.Contains(t, child.Text, "'source_file.dql','source_file'")
	require.NoError(t, os.WriteFile(child.Path, []byte(child.Text), 0600))
	for _, source := range []*Source{owner, child} {
		_, err := (Generator{Operation: "patch"}).Generate(t.Context(), GenerationRequest{Source: source, Destination: root})
		require.NoError(t, err)
	}
	snapshot := borrowedGeneratedSnapshot(t, root)
	_, err := (Generator{Operation: "patch"}).Generate(t.Context(), GenerationRequest{Source: child, Destination: root})
	require.NoError(t, err)
	require.Equal(t, snapshot, borrowedGeneratedSnapshot(t, root))
	borrowedGeneratedModuleTest(t, root)
	compiled, err := NewCompiler().Compile(t.Context(), child)
	require.NoError(t, err)
	found := false
	for _, ref := range compiled.Views {
		if ref == nil || ref.Borrowed == nil {
			continue
		}
		proof := ref.Borrowed
		require.Equal(t, "source_file", proof.Declaration.OwnerName)
		require.Equal(t, "Owner", proof.OwnerName)
		require.Equal(t, owner.Path, proof.OwnerSource)
		require.NoError(t, proof.ValidateFiles(func(path string) string { return path }))
		generated := filepath.Join(root, "generated")
		staged := generated + ".staged"
		require.NoError(t, os.Rename(generated, staged))
		err := proof.ValidateSchema()
		require.NoError(t, os.Rename(staged, generated))
		require.NoError(t, err, "schema revalidation must reuse captured component identity")
		found = true
	}
	require.True(t, found)
}
