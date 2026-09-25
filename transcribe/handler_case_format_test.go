package transcribe

import (
	"context"
	"os"
	"os/exec"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/viant/datly/internal/testharness"
	"github.com/viant/datly/transcribe/column"
	"github.com/viant/datly/transcribe/generate"
	fixture "github.com/viant/datly/transcribe/testdata/handleronly"
)

func TestHandlerOnlyCaseFormat(t *testing.T) {
	ctx := context.Background()
	root := t.TempDir()
	(testharness.GeneratedModule{Path: handlerFixtureModule}).Write(t, root)
	mapping := handlerMapping()
	mapping.FactoryName = "NewCaseHandler"
	binding, err := NewHandlerBinding[fixture.Input, fixture.CaseOutput](mapping, fixture.NewCaseHandler)
	require.NoError(t, err)
	require.NoError(t, os.Mkdir(filepath.Join(root, "dql"), 0700))
	source := handlerDQL + "\n#setting($_ = $case_format('lc'))"
	require.NoError(t, os.WriteFile(filepath.Join(root, "dql", "convert.dql"), []byte(source), 0600))
	db := &forbiddenHandlerDB{}
	discovery := Discovery{BaseDir: root, Include: []string{handlerFixtureModule + "/dql"}, HandlerBindings: []*HandlerBinding{binding}, ColumnRefiner: column.New(db)}
	before := fixture.Constructions.Load()
	for _, policy := range []generate.GenerationPolicy{"", "", generate.GenerationPolicyOverwrite} {
		project, err := discovery.Compile(ctx)
		require.NoError(t, err)
		require.Len(t, project.Components, 1)
		compiled := project.Components[0]
		require.Equal(t, "lc", compiled.Component.Settings.CaseFormat)
		require.Empty(t, compiled.Component.Settings.DefaultConnector)
		result, err := (Generator{Operation: "handler", GenerationPolicy: policy}).Generate(ctx, GenerationRequest{Compiled: compiled, Destination: root})
		require.NoError(t, err)
		require.Equal(t, "lc", result.Result.Plan.Settings.CaseFormat)
		require.Equal(t, generate.ContractLinked, result.Result.Plan.Output.Ownership)
		router, err := os.ReadFile(filepath.Join(root, "registration", "router.go"))
		require.NoError(t, err)
		require.Contains(t, string(router), `caseFormat:\"lc\"`)
		require.Contains(t, string(router), "handleronly.CaseOutput")
		require.NotContains(t, string(router), "view=")
	}
	require.Zero(t, db.calls)
	require.Equal(t, before, fixture.Constructions.Load())
	for source, destination := range map[string]string{
		"case_runtime_test.go.txt": "runtime_test.go",
		"case_reference.go.txt":    "reference.go",
	} {
		data, err := os.ReadFile(filepath.Join("testdata", "handleronly", source))
		require.NoError(t, err)
		require.NoError(t, os.WriteFile(filepath.Join(root, "registration", destination), data, 0600))
	}
	command := exec.CommandContext(ctx, "go", "test", "-mod=mod", "-count=1", "-timeout=2m", "./...")
	command.Dir = root
	out, err := command.CombinedOutput()
	require.NoError(t, err, "%s", out)
}
