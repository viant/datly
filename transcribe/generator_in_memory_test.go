package transcribe

import (
	"context"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/viant/datly/internal/testharness"
	"github.com/viant/datly/transcribe/column"
	"github.com/viant/datly/transcribe/generate"
)

func TestGeneratorInMemoryRelation(t *testing.T) {
	ctx := context.Background()
	db := testharness.NewSQLiteHarness(t)
	require.NoError(t, db.ExecStatements(ctx,
		"CREATE TABLE parents(id INTEGER, summary TEXT)",
		"CREATE TABLE lookup_values(feature_type TEXT, feature_value TEXT, score INTEGER)"))
	root := t.TempDir()
	const module = "github.com/viant/datly/inmemoryfixture"
	(testharness.GeneratedModule{Path: module}).Write(t, root)
	source := &Source{
		Name: "Parents", Scope: module + "/parents",
		ColumnRefiner: column.New(column.Connections{"main": db.DB}),
		Text: `#package('parents')
#setting($_ = $route('/parents', 'GET'))
#setting($_ = $input_type('ParentsInput'))
#setting($_ = $output_type('ParentsOutput'))
#set($_ = $Id<int>(query/id))
#set($_ = $Fields<[]string>(query/fields).QuerySelector(p).Optional())
#set($_ = $Status<?>(output/status).WithTag('anonymous:"true"'))
#set($_ = $Data<?>(output/view).WithTag('json:"data"'))
SELECT p.*, signals.*, perf.*, use_connector(p,'main'),
type(p,'ParentRow'), type(signals,'SignalRow'), type(perf,'PerformanceRow'),
in_memory(signals), allow_nulls(signals), set_limit(signals,40),
cardinality(signals,'Many'), cardinality(perf,'One'),
cast(p.id as int), cast(p.summary as string), tag(p.summary,'sqlx:"-"'),
cast(signals.parent_id as int), cast(signals.feature_type as string), cast(signals.feature_value as string), cast(signals.score as int),
cast(perf.feature_type as string), cast(perf.feature_value as string), cast(perf.score as int)
FROM (SELECT id, summary FROM parents WHERE id = $Id) p
JOIN (SELECT 0 AS parent_id, 'discovery_only' AS feature_type, '' AS feature_value, 0 AS score) signals ON p.id = signals.parent_id
JOIN (SELECT feature_type, feature_value, score FROM lookup_values) perf
ON signals.feature_type = perf.feature_type AND signals.feature_value = perf.feature_value`,
	}
	// Begin with the executable draft, then regenerate with the explicit
	// discovery-only declaration. No generated tag is edited by hand.
	draft := *source
	draft.Text = strings.Replace(source.Text, "in_memory(signals), ", "", 1)
	generator := Generator{Operation: "get", GenerationPolicy: generate.GenerationPolicyOverwrite}
	_, err := generator.Generate(ctx, GenerationRequest{Source: &draft, Destination: root})
	require.NoError(t, err)
	compiled, err := NewCompiler().Compile(ctx, source)
	require.NoError(t, err)
	child := compiled.Component.RootView.Relations[0].View
	require.True(t, child.InMemory)
	require.Contains(t, child.Source.SQL, "discovery_only")
	require.NotEmpty(t, child.Columns, "database discovery must still describe the intermediate row")
	// Changing an existing execution tag follows the existing explicit
	// migration policy. Overwrite is ownership-checked, not a manual edit.
	generator.GenerationPolicy = generate.GenerationPolicyMerge
	_, err = generator.Generate(ctx, GenerationRequest{Compiled: compiled, Destination: root})
	require.ErrorContains(t, err, "explicit migration required")
	generator.GenerationPolicy = generate.GenerationPolicyOverwrite
	generated, err := generator.Generate(ctx, GenerationRequest{Compiled: compiled, Destination: root})
	require.NoError(t, err)
	var holders []string
	for _, row := range generated.Result.Plan.Views {
		for _, field := range row.Fields {
			if field.Name == "Signals" {
				holders = append(holders, field.Name)
				require.Equal(t, "[]*SignalRow", field.Type)
				tags := reflect.StructTag(field.Tag)
				require.Empty(t, tags.Get("sql"))
				require.NotContains(t, tags.Get("view"), "table=")
				require.NotContains(t, tags.Get("view"), "uri=")
				require.Contains(t, tags.Get("view"), "allowNulls=true")
				require.Contains(t, tags.Get("view"), "limit=40")
				require.NotEmpty(t, tags.Get("on"))
			}
			if field.Name == "Perf" {
				holders = append(holders, field.Name)
				require.Equal(t, "*PerformanceRow", field.Type)
				require.NotEmpty(t, reflect.StructTag(field.Tag).Get("sql"))
			}
		}
	}
	require.ElementsMatch(t, []string{"Signals", "Perf"}, holders)
	for _, name := range []string{"hooks.go", "runtime_test.go"} {
		content, err := os.ReadFile("testdata/in_memory/" + name + ".txt")
		require.NoError(t, err)
		require.NoError(t, os.WriteFile(filepath.Join(root, "parents", name), content, 0600))
	}
	// Handwritten hooks remain separate and survive both persistent policies.
	for _, policy := range []generate.GenerationPolicy{generate.GenerationPolicyMerge, generate.GenerationPolicyOverwrite} {
		generator.GenerationPolicy = policy
		_, err = generator.Generate(ctx, GenerationRequest{Compiled: compiled, Destination: root})
		require.NoError(t, err)
		expected, err := os.ReadFile("testdata/in_memory/hooks.go.txt")
		require.NoError(t, err)
		actual, err := os.ReadFile(filepath.Join(root, "parents", "hooks.go"))
		require.NoError(t, err)
		require.Equal(t, expected, actual)
	}
	command := exec.CommandContext(ctx, "go", "test", "-mod=mod", "-count=1", "./...")
	command.Dir = root
	out, err := command.CombinedOutput()
	require.NoError(t, err, "%s", out)
}
