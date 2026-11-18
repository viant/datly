package transcribe

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestWrapperAuthoredBodyStableGeneration(t *testing.T) {
	root, source := generatedPostFactoryFixture(t)
	source.Text = strings.Replace(source.Text, "$Data<*archive.ArchiveRequest>(body/data).Optional()", `$Data<[]int>(body/).Optional().WithTag('jsonx:"wrapper=ids"')`, 1)
	path := filepath.Join(root, "archive/business.go")
	body, err := os.ReadFile(path)
	require.NoError(t, err)
	body = []byte(strings.Replace(string(body), `if in.Data != nil { for _, id := range in.Data.Ids { out.Results = append(out.Results, &ResultItem{Id:id,Status:"archived"}) } }`, `for _, id := range in.Data { out.Results = append(out.Results, &ResultItem{Id:id,Status:"archived"}) }`, 1))
	body = []byte(strings.Replace(string(body), "type ResultItem struct { Id string", "type ResultItem struct { Id int", 1))
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
		require.Contains(t, string(input), `jsonx:"wrapper=ids"`)
		require.Contains(t, string(input), "[]int")
		snapshot := sourceHandlerSnapshot(t, root)
		if first == nil {
			first = snapshot
		} else {
			require.Equal(t, first, snapshot, "regeneration must preserve authored wrapper contract")
		}
	}
}
