package transcribe

import (
	"context"
	"os"
	"os/exec"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/viant/datly/internal/testharness"
	"github.com/viant/datly/internal/testharness/sqlite"
	"github.com/viant/datly/transcribe/column"
)

func TestMySQLExecutionOptionRegeneration(t *testing.T) {
	ctx := context.Background()
	db := sqlite.New(t)
	require.NoError(t, db.ExecStatements(ctx, "CREATE TABLE audience(id INTEGER,name TEXT)"))
	root := t.TempDir()
	testharness.WriteGeneratedGoMod(t, root)
	source := &Source{Scope: "example.com/generated/hints", Name: "Audience", Connector: "main", ColumnRefiner: column.New(column.Connections{"main": db.DB}), Text: `#setting($_ = $route('/audience','GET'))
SELECT a.*, mysql_condition_fanout_filter(a, false) FROM audience a`}
	snapshot := map[string]string{}
	for generation := 0; generation < 2; generation++ {
		generated, err := transcribeSource(ctx, root, source)
		require.NoError(t, err)
		found := false
		for _, file := range generated.Result.Files {
			bytes, err := os.ReadFile(file.Path)
			require.NoError(t, err)
			if generation == 0 {
				snapshot[file.Path] = string(bytes)
			} else {
				require.Equal(t, snapshot[file.Path], string(bytes), file.Path)
			}
			found = found || strings.Contains(string(bytes), "mysqlConditionFanoutFilter=false")
			require.NotContains(t, string(bytes), "mysql_condition_fanout_filter(")
			require.NotContains(t, string(bytes), "SET_VAR(")
		}
		require.True(t, found, "generated metadata must retain the option")
	}
	// A malformed declaration must not publish over the existing artifacts.
	source.Text = strings.ReplaceAll(source.Text, "filter(a, false)", "filter(a, true)")
	_, err := transcribeSource(ctx, root, source)
	require.ErrorContains(t, err, "only false")
	for path, before := range snapshot {
		bytes, err := os.ReadFile(path)
		require.NoError(t, err)
		require.Equal(t, before, string(bytes))
	}
	source.Text = strings.ReplaceAll(source.Text, ", mysql_condition_fanout_filter(a, true)", "")
	removed, err := transcribeSource(ctx, root, source)
	require.NoError(t, err)
	for _, file := range removed.Result.Files {
		bytes, err := os.ReadFile(file.Path)
		require.NoError(t, err)
		require.NotContains(t, string(bytes), "mysqlConditionFanoutFilter")
	}
	command := exec.Command("go", "test", "-mod=mod", "./...")
	command.Dir = root
	output, err := command.CombinedOutput()
	require.NoError(t, err, string(output))
}
