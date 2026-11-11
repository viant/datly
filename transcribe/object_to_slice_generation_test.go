package transcribe

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestObjectToSliceAuthoredBodyStableGeneration(t *testing.T) {
	root, source := generatedPostFactoryFixture(t)
	source.Text = strings.Replace(source.Text, "$Data<*archive.ArchiveRequest>(body/data).Optional()", "$Data<[]archive.ArchiveRequest>(body/).Optional().WithTag('jsonx:\"objectToSlice\"')", 1)
	path := filepath.Join(root, "archive/business.go")
	body, err := os.ReadFile(path)
	require.NoError(t, err)
	// The handler consumes the authored collection directly, without a JSON codec.
	body = []byte(strings.Replace(string(body), "if in.Data != nil { for _, id := range in.Data.Ids { out.Results = append(out.Results, &ResultItem{Id:id,Status:\"archived\"}) } }", "for _, row := range in.Data { for _, id := range row.Ids { out.Results = append(out.Results, &ResultItem{Id:id,Status:\"archived\"}) } }", 1))
	require.NoError(t, os.WriteFile(path, body, 0600))
	var first map[string]string
	discovery := Discovery{BaseDir: root, GoBuild: source.GoBuild}
	for range 2 {
		compiled, err := discovery.CompileSource(t.Context(), source)
		require.NoError(t, err)
		_, err = (Generator{Operation: "post"}).Generate(t.Context(), GenerationRequest{Compiled: compiled, Destination: root})
		require.NoError(t, err)
		input, err := os.ReadFile(filepath.Join(root, "archive/input.go"))
		require.NoError(t, err)
		require.Contains(t, string(input), `jsonx:"objectToSlice"`)
		require.Contains(t, string(input), "[]ArchiveRequest")
		snapshot := sourceHandlerSnapshot(t, root)
		if first == nil {
			first = snapshot
		} else {
			require.Equal(t, first, snapshot, "regeneration must preserve the complete contract")
		}
	}
}
