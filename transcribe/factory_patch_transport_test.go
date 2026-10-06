package transcribe

import (
	"context"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/viant/datly/transcribe/column"
)

func TestGeneratedFactoryPatchTransport(t *testing.T) {
	for _, qualified := range []bool{false, true} {
		t.Run(fmt.Sprintf("qualified_%v", qualified), func(t *testing.T) {
			root, source := generatedPostFactoryFixture(t)
			source.Text = strings.Replace(source.Text, "'/archive','POST'", "'/archive','PATCH'", 1)
			source.Text += "\n#define($_ = $Mode<string>(body/mode).Optional())"
			businessFile := filepath.Join(root, "archive/business.go")
			business, err := os.ReadFile(businessFile)
			require.NoError(t, err)
			business = []byte(strings.Replace(string(business), `out.Status = "ok"`, `out.Status = "ok"; if in.Mode != "" { out.Status = in.Mode }`, 1))
			require.NoError(t, os.WriteFile(businessFile, business, 0600))
			if !qualified {
				source.Text = strings.Replace(source.Text, "'archive.NewArchive'", "'NewArchive'", 1)
			}
			before := sourceHandlerSnapshot(t, root)
			db := &forbiddenHandlerDB{}
			discovery := Discovery{BaseDir: root, GoBuild: source.GoBuild, ColumnRefiner: column.New(db)}
			var first map[string]string
			for round := range 2 {
				compiled, err := discovery.CompileSource(context.Background(), source)
				require.NoError(t, err)
				require.Nil(t, compiled.Component.RootView)
				require.Empty(t, compiled.Component.Views)
				require.Empty(t, compiled.Component.Settings.Mutation)
				require.Equal(t, "PATCH", compiled.Component.Routes[0].Method)
				require.True(t, compiled.ExternalHandler.GeneratedContracts)
				request := GenerationRequest{Compiled: compiled, Destination: root}
				if round == 1 {
					request = GenerationRequest{Source: compiled.Source, Destination: root}
				}
				generated, err := (Generator{Operation: "post"}).Generate(context.Background(), request)
				require.NoError(t, err)
				require.Nil(t, generated.Result.Plan.MutationHandler)
				require.Empty(t, generated.Result.Plan.Views)
				current := sourceHandlerSnapshot(t, root)
				require.Equal(t, before[filepath.Join(root, "archive/business.go")], current[filepath.Join(root, "archive/business.go")])
				if first == nil {
					first = current
				} else {
					require.Equal(t, first, current)
				}
				for _, op := range []string{"get", "put", "patch", "handler"} {
					_, err = (Generator{Operation: op}).Generate(context.Background(), GenerationRequest{Compiled: compiled, Destination: root})
					require.Error(t, err)
					require.Equal(t, current, sourceHandlerSnapshot(t, root))
				}
			}
			require.Zero(t, db.calls)
			runtimeSource := strings.ReplaceAll(generatedPostFactoryRuntimeTest, `httptest.NewRequest("POST"`, `httptest.NewRequest("PATCH"`)
			runtimeSource = strings.Replace(runtimeSource, "  } {", `   {"{\"mode\":\"selected\",\"data\":{\"ids\":[\"scalar\"]}}","{\"status\":\"selected\",\"results\":[{\"id\":\"scalar\",\"status\":\"archived\"}]}"},
  } {`, 1)

			runtimeSource = strings.Replace(runtimeSource, ` "testing"`, ` "testing"
 dexec "github.com/viant/datly/exec"
 "github.com/viant/datly/spec"`, 1)
			runtimeSource = strings.Replace(runtimeSource, "  cancel()", `
  typed, invokeErr := server.InvokeComponent(ctx,dexec.ComponentRequest{Target:dexec.ComponentTarget{Component:spec.Key{Kind:spec.KindComponent,Scope:"github.com/viant/datly/handlerfixture/archive",Name:"Archive"},Route:spec.RouteRef{Method:"PATCH",Path:"/archive"}},Input:&ArchiveInput{Mode:"typed-selected",Data:&ArchiveRequest{Ids:[]string{"typed"}}}})
  require.NoError(t,invokeErr)
  require.Equal(t,"typed",typed.(*ArchiveOutput).Results[0].Id)
  require.Equal(t,"typed-selected",typed.(*ArchiveOutput).Status)
  cancel()`, 1)
			writeSourceHandlerFile(t, root, "archive/runtime_test.go", runtimeSource)
			cmd := exec.CommandContext(t.Context(), "go", "test", "-mod=readonly", "-race", "-count=1", "-timeout=2m", "./archive")
			cmd.Dir = root
			output, err := cmd.CombinedOutput()
			require.NoError(t, err, "%s", output)
		})
	}
}

func generatedFactoryTransportFixture(t *testing.T, method string) (string, *Source) {
	t.Helper()
	root, source := generatedPostFactoryFixture(t)
	source.Text = strings.Replace(source.Text, "'/archive','POST'", "'/archive','"+method+"'", 1)
	return root, source
}
