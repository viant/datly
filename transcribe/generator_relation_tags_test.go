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

func TestGeneratorHiddenRelationHolders(t *testing.T) {
	ctx := context.Background()
	db := testharness.NewSQLiteHarness(t)
	require.NoError(t, db.ExecStatements(ctx,
		"CREATE TABLE parents(id INTEGER, name TEXT)",
		"CREATE TABLE timeline(parent_id INTEGER, value INTEGER)",
		"CREATE TABLE summaries(parent_id INTEGER, value INTEGER)"))
	root := t.TempDir()
	const module = "github.com/viant/datly/relationtagsfixture"
	(testharness.GeneratedModule{Path: module}).Write(t, root)
	source := &Source{Name: "Parents", Scope: module + "/parents", Connector: "main",
		ColumnRefiner: column.New(column.Connections{"main": db.DB}), Text: `#package('parents')
#setting($_ = $route('/parents','GET'))
#setting($_ = $mcp('Parents'))
#setting($_ = $case_format('lc'))
#setting($_ = $input_type('ParentsInput'))
#setting($_ = $output_type('ParentsOutput'))
#set($_ = $Id<int>(query/id).WithTag('json:"id"'))
#set($_ = $Fields<[]string>(query/fields).QuerySelector(p).WithTag('json:"fields"').Optional())
#set($_ = $Status<?>(output/status).WithTag('anonymous:"true"'))
#set($_ = $Data<?>(output/view).WithTag('json:"data"'))
SELECT p.*, privateTimeline.*, privateSummary.*,
type(p,'ParentRow'), type(privateTimeline,'TimelineRow'), type(privateSummary,'SummaryRow'),
tag(privateTimeline,'json:"-" internal:"true"'),
tag(privateSummary,'json:"-" internal:"true"'),
cardinality(privateSummary,'One'),
cast(p.id AS int), cast(p.name AS string), cast(p.computed AS int),
cast(privateTimeline.parent_id AS int), cast(privateTimeline.value AS int),
cast(privateSummary.parent_id AS int), cast(privateSummary.value AS int)
FROM (SELECT id,name,0 AS computed FROM parents WHERE id=$Id) p
LEFT JOIN (SELECT parent_id,value FROM timeline) privateTimeline ON p.id=privateTimeline.parent_id
LEFT JOIN (SELECT parent_id,value FROM summaries) privateSummary ON p.id=privateSummary.parent_id`,
	}
	generator := Generator{Operation: "get"}
	// Changing existing visibility follows the explicit shape-migration policy;
	// ownership-checked overwrite needs no edits to generated Go.
	draft := *source
	draft.Text = strings.ReplaceAll(draft.Text, `tag(privateTimeline,'json:"-" internal:"true"'),`, "")
	draft.Text = strings.ReplaceAll(draft.Text, `tag(privateSummary,'json:"-" internal:"true"'),`, "")
	_, err := generator.Generate(ctx, GenerationRequest{Source: &draft, Destination: root})
	require.NoError(t, err)
	_, err = generator.Generate(ctx, GenerationRequest{Source: source, Destination: root})
	require.ErrorContains(t, err, "explicit migration required")
	generator.GenerationPolicy = generate.GenerationPolicyOverwrite
	generated, err := generator.Generate(ctx, GenerationRequest{Source: source, Destination: root})
	require.NoError(t, err)
	holders := 0
	for _, view := range generated.Result.Plan.Views {
		for _, field := range view.Fields {
			if !field.RelationHolder {
				continue
			}
			holders++
			tags := reflect.StructTag(field.Tag)
			require.Equal(t, "-", tags.Get("json"))
			require.Equal(t, "true", tags.Get("internal"))
			require.NotEmpty(t, tags.Get("sql"))
			require.NotEmpty(t, tags.Get("on"))
		}
	}
	require.Equal(t, 2, holders)
	viewsFile := filepath.Join(root, "parents", generated.Result.Plan.ViewDest)
	before, err := os.ReadFile(viewsFile)
	require.NoError(t, err)
	for _, name := range []string{"hooks.go", "runtime_test.go"} {
		content, err := os.ReadFile("testdata/relation_tags/" + name + ".txt")
		require.NoError(t, err)
		require.NoError(t, os.WriteFile(filepath.Join(root, "parents", name), content, 0600))
	}
	for _, policy := range []generate.GenerationPolicy{generate.GenerationPolicyMerge, generate.GenerationPolicyOverwrite} {
		generator.GenerationPolicy = policy
		_, err = generator.Generate(ctx, GenerationRequest{Source: source, Destination: root})
		require.NoError(t, err)
		after, err := os.ReadFile(viewsFile)
		require.NoError(t, err)
		require.Equal(t, string(before), string(after))
	}
	command := exec.CommandContext(ctx, "go", "test", "-mod=mod", "-count=1", "-timeout=120s", "-v", "./...")
	command.Dir = root
	command.Env = append(os.Environ(), "GOWORK=off")
	out, err := command.CombinedOutput()
	require.NoError(t, err, "%s", out)
	t.Logf("generated holder regression:\n%s", out)
}
