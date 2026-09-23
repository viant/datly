package transcribe

import (
	"context"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/viant/datly/internal/testharness"
	"github.com/viant/datly/transcribe/column"
	"github.com/viant/datly/transcribe/generate"
)

func TestGeneratorPrivateReader(t *testing.T) {
	ctx := context.Background()
	db := testharness.NewSQLiteHarness(t)
	require.NoError(t, db.ExecStatements(ctx, "CREATE TABLE records(id INTEGER, name TEXT)"))
	root := t.TempDir()
	const module = "github.com/viant/datly/privatereaderfixture"
	(testharness.GeneratedModule{Path: module}).Write(t, root)
	source := &Source{Name: "PrivateRead", Scope: module + "/records", Connector: "main",
		ColumnRefiner: column.New(column.Connections{"main": db.DB}), Text: `#package('records')
#setting($_ = $route('/api/private/read','GET'))
#setting($_ = $internal(true))
#setting($_ = $api_key('X-Reader-Key','secret'))
#setting($_ = $input_type('PrivateInput'))
#setting($_ = $output_type('PrivateOutput'))
#setting($_ = $case_format('lc'))
#setting($_ = $cache(true,'1m'))
#setting($_ = $cache_warmup('id'))
#set($_ = $Status<?>(output/status).WithTag('anonymous:"true"'))
#set($_ = $Data<?>(output/view).WithTag('json:"data"'))
SELECT r.id,r.name FROM records r`,
	}
	public := *source
	public.Text = strings.ReplaceAll(public.Text, "$internal(true)", "$internal(false)")
	_, err := (Generator{Operation: "get"}).Generate(ctx, GenerationRequest{Source: &public, Destination: root})
	require.NoError(t, err)
	for _, policy := range []generate.GenerationPolicy{"", "", generate.GenerationPolicyOverwrite} {
		generated, err := (Generator{Operation: "get", GenerationPolicy: policy}).Generate(ctx, GenerationRequest{Source: source, Destination: root})
		require.NoError(t, err)
		require.Len(t, generated.Result.Plan.Routes, 1)
		require.True(t, generated.Result.Plan.Routes[0].Internal)
		router, err := os.ReadFile(filepath.Join(root, "records", "router.go"))
		require.NoError(t, err)
		require.Contains(t, string(router), "internal=true")
		require.Contains(t, string(router), "EmbedNamespace", "policy=%q router=%s", policy, router)
	}
	runtimeTest, err := os.ReadFile("testdata/private_reader/runtime_test.go.txt")
	require.NoError(t, err)
	require.NoError(t, os.WriteFile(filepath.Join(root, "records", "private_test.go"), runtimeTest, 0600))
	command := exec.CommandContext(ctx, "go", "test", "-mod=mod", "-count=1", "-timeout=2m", "./...")
	command.Dir = root
	out, err := command.CombinedOutput()
	require.NoError(t, err, "%s", out)
}
