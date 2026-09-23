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
)

func TestGeneratorPostReader(t *testing.T) {
	ctx := context.Background()
	db := testharness.NewSQLiteHarness(t)
	require.NoError(t, db.ExecStatements(ctx,
		"CREATE TABLE profiles(id INTEGER, category TEXT, name TEXT)",
		"INSERT INTO profiles VALUES (1,'a','keep'),(2,'a','omit'),(3,'b','other')"))
	root := t.TempDir()
	const module = "github.com/viant/datly/postreaderfixture"
	(testharness.GeneratedModule{Path: module}).Write(t, root)
	source := &Source{
		Name: "Profiles", Scope: module + "/profiles", Connector: "main",
		ColumnRefiner: column.New(column.Connections{"main": db.DB}),
		Text: `#package('profiles')
#setting($_ = $route('/profiles/read', 'POST'))
#setting($_ = $input_type('ProfilesInput'))
#setting($_ = $output_type('ProfilesOutput'))
#set($_ = $Inclusion<string>(body/inclusion))
#set($_ = $Exclusion<string>(body/exclusion))
#set($_ = $Fields<[]string>(query/fields).QuerySelector(p).Optional())
#set($_ = $Status<?>(output/status).WithTag('anonymous:"true"'))
#set($_ = $Metrics<?>(output/metrics).WithTag('json:"metrics"'))
#set($_ = $Data<?>(output/view).WithTag('json:"data"'))
SELECT p.id, p.name FROM profiles p WHERE p.category = $Inclusion AND p.name <> $Exclusion ORDER BY p.id`,
	}
	generator := Generator{Operation: "get", GenerationPolicy: generate.GenerationPolicyOverwrite}
	// Cover direct generation and the discovery-then-generation path used by
	// application wrappers, including persistent overwrite regeneration.
	compiled, err := NewCompiler().Compile(ctx, source)
	require.NoError(t, err)
	for _, request := range []GenerationRequest{
		{Source: source, Destination: root},
		{Compiled: compiled, Destination: root},
	} {
		generated, err := generator.Generate(ctx, request)
		require.NoError(t, err)
		plan := generated.Result.Plan
		require.Equal(t, "POST", compiled.Component.Routes[0].Method)
		require.Empty(t, plan.Settings.Mutation)
		require.Nil(t, plan.MutationHandler)
		require.Nil(t, plan.EntitySupport)
		require.Nil(t, plan.HookScaffold)
		for _, binding := range []struct{ field, source string }{{"Inclusion", "inclusion"}, {"Exclusion", "exclusion"}} {
			field, ok := plan.Input.Field(binding.field)
			require.True(t, ok)
			require.Equal(t, "body", field.Source)
			require.Contains(t, field.Tag, "in="+binding.source)
		}
	}
	// Mutation generation must still reject ambiguous mutation bodies rather
	// than guessing that multiple body parameters imply reader intent.
	_, err = (Generator{Operation: "post"}).Generate(ctx, GenerationRequest{Compiled: compiled, Destination: root})
	require.ErrorContains(t, err, "generation body input is ambiguous")
	runtimeTest, err := os.ReadFile("testdata/post_reader/runtime_test.go.txt")
	require.NoError(t, err)
	require.NoError(t, os.WriteFile(filepath.Join(root, "profiles", "reader_test.go"), runtimeTest, 0600))
	command := exec.CommandContext(ctx, "go", "test", "-mod=mod", "-count=1", "./...")
	command.Dir = root
	out, err := command.CombinedOutput()
	require.NoError(t, err, "%s", out)
}
