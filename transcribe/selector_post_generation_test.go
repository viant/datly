package transcribe

import (
	"context"
	"github.com/stretchr/testify/require"
	"github.com/viant/datly/internal/testharness"
	"github.com/viant/datly/transcribe/column"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestPostReaderSelectorGenerationReloadAndRepeat(t *testing.T) {
	ctx := context.Background()
	root := t.TempDir()
	testharness.WriteGeneratedGoMod(t, root)
	db := testharness.NewSQLiteHarness(t)
	require.NoError(t, db.ExecStatements(ctx, "CREATE TABLE records(id INTEGER,label TEXT)"))
	refiner := column.New(column.Connections{"main": db.DB})
	const text = `#package('example.com/generated/rows')
#setting($_ = $route('/rows','POST'))
#define($_ = $Minimum<int>(body/minimum).Optional())
#define($_ = $ID<int>(query/id).Optional())
#define($_ = $Page<int>(query/page).QuerySelector('r'))
#define($_ = $Limit<int>(query/limit).QuerySelector('r'))
#define($_ = $Offset<int>(query/offset).QuerySelector('r'))
#define($_ = $OrderBy<[]string>(query/sort).QuerySelector('r'))
SELECT r.*,CAST(r.id AS int),type(r,'Row') FROM (SELECT id,label FROM records WHERE id > $Minimum AND id >= $ID) r`
	source := &Source{Name: "Rows", Scope: "example.com/generated/source", Text: text, Connector: "main", ColumnRefiner: refiner}
	result, err := NewCompiler().Compile(ctx, source)
	require.NoError(t, err)
	_, err = (Generator{Operation: "get"}).Generate(ctx, GenerationRequest{Compiled: result, Destination: root})
	require.NoError(t, err)
	snapshot := func() map[string]string {
		out := map[string]string{}
		require.NoError(t, filepath.WalkDir(filepath.Join(root, "rows"), func(path string, entry os.DirEntry, err error) error {
			if err != nil {
				return err
			}
			if !entry.IsDir() {
				b, e := os.ReadFile(path)
				if e != nil {
					return e
				}
				out[path] = string(b)
			}
			return nil
		}))
		return out
	}
	before := snapshot()
	var holder string
	for _, body := range before {
		if strings.Contains(body, "DatlyQuerySelectors()") {
			holder = body
		}
	}
	for _, wanted := range []string{"Page int", "Limit int", "Offset int", "Sort []string", "method=POST"} {
		require.Contains(t, strings.Join(strings.Fields(holder), " "), wanted)
	}
	for path, body := range before {
		if strings.HasSuffix(path, "input.go") || strings.HasSuffix(path, "input_setters.go") {
			for _, name := range []string{"Page", "Limit", "Offset", "Sort"} {
				require.NotContains(t, body, name)
			}
		}
	}
	packageOnly := &Discovery{BaseDir: root, Include: []string{"example.com/generated/rows"}}
	project, err := packageOnly.Compile(ctx)
	require.NoError(t, err)
	require.Len(t, project.Components, 1)
	require.NotNil(t, project.Components[0].Component.SelectorCompanion)
	// Recompile authored source against the emitted package's current contracts.
	writeSourceFile(t, root, "source/Rows.dql", text)
	authored := &Discovery{BaseDir: root, Include: []string{"example.com/generated/source"}, SourceFile: "Rows.dql", Connector: "main", ColumnRefiner: refiner}
	project, err = authored.Compile(ctx)
	require.NoError(t, err)
	require.Len(t, project.Components, 1)
	_, err = (Generator{Operation: "get"}).Generate(ctx, GenerationRequest{Compiled: project.Components[0], Destination: root})
	require.NoError(t, err)
	require.Equal(t, before, snapshot(), "package reload and repeat must preserve native artifacts")
}
