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
	"github.com/viant/datly/spec"
	"github.com/viant/datly/transcribe/column"
)

func TestTranscribedCubeWordBoundaries(t *testing.T) {
	root := t.TempDir()
	testharness.WriteGeneratedGoMod(t, root)
	no := false
	var columns []*spec.Column
	names := []string{"VStart", "V100", "ViewConversions", "AccountID", "IsPG", "URL", "V25Threshold", "V100Window", "ControlVExposed"}
	sqlNames := []string{"v_start", "v100", "view_conversions", "account_id", "is_pg", "url", "v25_threshold", "v100_window", "control_v_exposed"}
	wires := []string{"vStart", "v100", "viewConversions", "accountId", "isPg", "url", "v25Threshold", "v100Window", "controlVExposed", "accountIds"}
	var dql strings.Builder
	dql.WriteString("#setting($_ = $route('/metrics','GET'))\n#setting($_ = $report())\n")
	for i, name := range names {
		dql.WriteString("#define($_ = $" + name + "<int>(query/" + sqlNames[i] + ").Optional())\n")
		groupable := i != 0 && i != 2 && i != 7
		columns = append(columns, &spec.Column{Name: name, Type: spec.TypeRef{Name: "int"}, Groupable: &groupable, Tag: `json:"reader_` + sqlNames[i] + `"`})
	}
	dql.WriteString("#define($_ = $AccountIDs<[]int>(query/account_ids).Optional())\n")
	columns = append(columns, &spec.Column{Name: "Hidden", Type: spec.TypeRef{Name: "int"}, Groupable: &no, Tag: `json:"-"`})
	dql.WriteString("SELECT e.v_start AS VStart,e.v100 AS V100,e.view_conversions AS ViewConversions,e.account_id AS AccountID,e.is_pg AS IsPG,e.url AS URL,e.v25_threshold AS V25Threshold,e.v100_window AS V100Window,e.control_v_exposed AS ControlVExposed,e.hidden AS Hidden,type(e,'MetricsRow'),groupable(e) FROM events e")
	source := &Source{Scope: "example.com/generated/metrics", Name: "Metrics", Text: dql.String(), PreparedColumns: func(context.Context, column.PreparedColumnRequest) (*column.PreparedColumnResult, error) {
		return &column.PreparedColumnResult{Columns: columns}, nil
	}}
	snapshot := map[string]string{}
	for generation := 0; generation < 2; generation++ {
		generated, err := transcribeSource(context.Background(), root, source)
		require.NoError(t, err)
		cubePath := ""
		for _, file := range generated.Result.Files {
			if strings.HasSuffix(file.Path, "_cube.go") {
				cubePath = file.Path
			}
			content, err := os.ReadFile(file.Path)
			require.NoError(t, err)
			if generation == 0 {
				snapshot[file.Path] = string(content)
			} else {
				require.Equal(t, snapshot[file.Path], string(content), file.Path)
			}
		}
		require.Len(t, generated.Result.Files, len(snapshot))
		require.NotEmpty(t, cubePath, "%+v", generated.Result.Files)
		content, err := os.ReadFile(cubePath)
		require.NoError(t, err)
		for _, wire := range wires {
			require.Contains(t, string(content), `json:"`+wire+`,omitempty"`)
		}
		require.NotContains(t, string(content), `json:"vsTart`)
		for _, invalid := range []string{"v25tHreshold", "v100wIndow", "controlVeXposed", "accountIdS"} {
			require.NotContains(t, string(content), `json:"`+invalid)
		}
		runtimeTest, err := os.ReadFile("testdata/cube_word_boundary/runtime_test.go.txt")
		require.NoError(t, err)
		if generation == 1 {
			require.NoError(t, os.WriteFile(filepath.Join(filepath.Dir(cubePath), "word_boundary_test.go"), []byte(strings.ReplaceAll(string(runtimeTest), "CUBE_PACKAGE", generated.Result.Plan.PackageName())), 0600))
		}
	}
	command := exec.Command("go", "test", "-mod=mod", "-race", "-timeout=1m", "./...")
	command.Dir = root
	command.Env = append(os.Environ(), "GOWORK=off")
	output, err := command.CombinedOutput()
	require.NoError(t, err, string(output))
}
