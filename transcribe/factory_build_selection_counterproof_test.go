package transcribe

import (
	"context"
	"encoding/json"
	"github.com/stretchr/testify/require"
	"github.com/viant/datly/spec"
	"os"
	"os/exec"
	"path/filepath"
	"testing"
)

// This records the pre-existing native import-loader limit, not sharing acceptance.
func TestGeneratedPostFactoryExcludedContractImportCounterproof(t *testing.T) {
	for _, tc := range []struct{ name, file, env string }{{"GOOS", "extra_windows.go", "GOOS=linux"}, {"GOARCH", "extra_amd64.go", "GOARCH=arm64"}} {
		t.Run(tc.name, func(t *testing.T) {
			root, source := generatedPostFactoryFixture(t)
			compiled, err := (&Discovery{BaseDir: root, GoBuild: source.GoBuild}).CompileSource(context.Background(), source)
			require.NoError(t, err)
			_, err = (Generator{Operation: "post"}).Generate(t.Context(), GenerationRequest{Compiled: compiled, Destination: root})
			require.NoError(t, err)
			writeSourceHandlerFile(t, root, "archive/"+tc.file, "package archive\ntype ArchiveInput struct{}\n")
			source.GoBuild = source.GoBuild.Clone()
			source.GoBuild.Env = append(source.GoBuild.Env, tc.env, "CGO_ENABLED=0")
			cmd := exec.CommandContext(t.Context(), "go", "list", "-e", "-json", "./archive")
			cmd.Dir = root
			cmd.Env = append(os.Environ(), source.GoBuild.Env...)
			out, err := cmd.Output()
			require.NoError(t, err)
			var selected struct{ GoFiles []string }
			require.NoError(t, json.Unmarshal(out, &selected))
			require.NotContains(t, selected.GoFiles, tc.file)
			ownership := &factoryDeclarations{ctx: t.Context(), build: source.GoBuild, pkg: handlerFixtureModule + "/archive"}
			require.NoError(t, ownership.validateShape("Archive", &spec.View{TypeName: "ArchiveInput", Dest: "input.go"}))
			before := sourceHandlerSnapshot(t, root)
			_, err = (&Discovery{BaseDir: root, GoBuild: source.GoBuild}).CompileSource(t.Context(), source)
			require.ErrorContains(t, err, "register DQL import")
			require.ErrorContains(t, err, "more than once")
			require.Equal(t, before, sourceHandlerSnapshot(t, root))
			t.Logf("Go-selected files %v exclude %s; original native loader rejects: %v", selected.GoFiles, tc.file, err)
			require.NoError(t, os.Remove(filepath.Join(root, "archive", tc.file)))
		})
	}
}
