package transcribe

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/viant/datly/internal/testharness"
	"github.com/viant/datly/transcribe/column"
)

// A new ordinary input must be discovered from current DQL even when the
// previous generated component also carries a separate query-selector shape.
func TestGeneratedSelectorInputRefresh(t *testing.T) {
	ctx := context.Background()
	root := t.TempDir()
	(testharness.GeneratedModule{Path: "example.com/inputrefresh"}).Write(t, root)
	h := testharness.NewSQLiteHarness(t)
	require.NoError(t, h.ExecStatements(ctx, "CREATE TABLE records(id INTEGER, provider_id INTEGER)"))
	refiner := column.New(column.Connections{"main": h.DB})
	text := `#package('example.com/inputrefresh/records')
#setting($_ = $route('/records','GET'))
#setting($_ = $input_type('RecordsInput'))
#setting($_ = $output_type('RecordsOutput'))
#define($_ = $ID<int>(query/id).Optional().WithPredicate(0,'equal','r','id'))
#define($_ = $Page<int>(query/page).QuerySelector('r'))
SELECT r.* FROM (SELECT id,provider_id FROM records) r`
	compile := func() *Result {
		writeSourceFile(t, root, "source/records.dql", text)
		p, err := (&Discovery{BaseDir: root, Include: []string{"example.com/inputrefresh/source"}, SourceFile: "records.dql", Connector: "main", ColumnRefiner: refiner}).Compile(ctx)
		require.NoError(t, err)
		require.Len(t, p.Components, 1)
		return p.Components[0]
	}
	_, err := (Generator{Operation: "get"}).Generate(ctx, GenerationRequest{Compiled: compile(), Destination: root})
	require.NoError(t, err)
	text = strings.Replace(text, "SELECT r.*", "#define($_ = $ProviderIDs<[]int>(query/providerIds).Optional().WithPredicate(0,'in','r','provider_id'))\nSELECT r.*", 1)
	refreshed, err := (Generator{Operation: "get"}).Generate(ctx, GenerationRequest{Compiled: compile(), Destination: root})
	require.NoError(t, err)
	_, present := refreshed.Result.Plan.Input.Field("ProviderIDs")
	require.True(t, present)
	_, present = refreshed.Result.Plan.Input.Field("Page")
	require.False(t, present, "selectors remain separate from ordinary input")
	path := filepath.Join(root, "records", "input.go")
	before, err := os.ReadFile(path)
	require.NoError(t, err)
	_, err = (Generator{Operation: "get"}).Generate(ctx, GenerationRequest{Compiled: compile(), Destination: root})
	require.NoError(t, err)
	after, err := os.ReadFile(path)
	require.NoError(t, err)
	require.Equal(t, string(before), string(after), "repeated generation is stable")
}
