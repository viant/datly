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

func TestGeneratedSelectorCompanionAddsNamespacedBindings(t *testing.T) {
	ctx := context.Background()
	root := t.TempDir()
	testharness.WriteGeneratedGoMod(t, root)
	db := testharness.NewSQLiteHarness(t)
	require.NoError(t, db.ExecStatements(ctx, "CREATE TABLE records(id INTEGER,label TEXT)"))
	refiner := column.New(column.Connections{"main": db.DB})
	const initial = `#package('example.com/generated/rows')
#setting($_ = $route('/rows','GET'))
#setting($_ = $input_type('Input'))
#setting($_ = $output_type('Output'))
#define($_ = $ID<int>(query/id).Optional())
#define($_ = $Fields<[]string>(query/fields).QuerySelector('r'))
#define($_ = $Page<int>(query/page).QuerySelector('r'))
SELECT r.*,CAST(r.id AS int),type(r,'Row') FROM (SELECT id,label FROM records WHERE id >= $ID) r`
	result, err := NewCompiler().Compile(ctx, &Source{Name: "Rows", Scope: "example.com/generated/source", Text: initial, Connector: "main", ColumnRefiner: refiner})
	require.NoError(t, err)
	_, err = (Generator{Operation: "get"}).Generate(ctx, GenerationRequest{Compiled: result, Destination: root})
	require.NoError(t, err)
	expanded := strings.Replace(initial, "SELECT r.*", `#define($_ = $Limit<int>(query/r_limit).QuerySelector('r'))
#define($_ = $Offset<int>(query/r_offset).QuerySelector('r'))
#define($_ = $OrderBy<[]string>(query/r_orderby).QuerySelector('r'))
SELECT r.*`, 1)
	writeSourceFile(t, root, "source/Rows.dql", expanded)
	compile := func() *ProjectGeneration {
		project, e := (&Discovery{BaseDir: root, Include: []string{"example.com/generated/source"}, SourceFile: "Rows.dql", Connector: "main", ColumnRefiner: refiner}).Compile(ctx)
		require.NoError(t, e)
		require.Len(t, project.Components, 1)
		return project
	}
	project := compile()
	_, err = (Generator{Operation: "get"}).Generate(ctx, GenerationRequest{Compiled: project.Components[0], Destination: root})
	require.NoError(t, err)
	snapshot := func() map[string]string {
		out := map[string]string{}
		require.NoError(t, filepath.WalkDir(filepath.Join(root, "rows"), func(path string, entry os.DirEntry, e error) error {
			if e != nil {
				return e
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
	for path, body := range before {
		if strings.Contains(body, "DatlyQuerySelectors()") {
			holder = body
		}
		if strings.HasSuffix(path, "input.go") || strings.HasSuffix(path, "input_setters.go") {
			for _, name := range []string{"Fields", "Page", "Limit", "Offset", "OrderBy"} {
				require.NotContains(t, body, name)
			}
		}
	}
	require.NotEmpty(t, holder)
	for _, tag := range []string{"in=r_limit", "in=r_offset", "in=r_orderby"} {
		require.Contains(t, holder, tag)
	}
	loaded, err := (&Discovery{BaseDir: root, Include: []string{"example.com/generated/rows"}}).Compile(ctx)
	require.NoError(t, err)
	require.Len(t, loaded.Components, 1)
	require.NotNil(t, loaded.Components[0].Component.SelectorCompanion)
	input, err := (&discoveryInputCompiler{component: loaded.Components[0].Component, source: loaded.Components[0].Source, resolver: loaded.Components[0].TypeResolver, declarations: loaded.Components[0].Declarations, viewBindings: loaded.Components[0].ViewBindings, views: loaded.Components[0].Views}).compile()
	require.NoError(t, err)
	for _, name := range []string{"Fields", "Page", "Limit", "Offset", "OrderBy"} {
		_, exists := input.Value.Type().FieldByName(name)
		require.False(t, exists)
	}
	project = compile()
	_, err = (Generator{Operation: "get"}).Generate(ctx, GenerationRequest{Compiled: project.Components[0], Destination: root})
	require.NoError(t, err)
	require.Equal(t, before, snapshot())
}
