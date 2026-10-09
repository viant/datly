package column_test

import (
	"context"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/viant/datly/internal/testharness/sqlite"
	"github.com/viant/datly/spec"
	"github.com/viant/datly/transcribe/column"
	"github.com/viant/datly/transcribe/generate"
)

// Keep generation parity outside package column: generation itself depends on
// compilation and column refinement.
func TestCompilationMetadataGeneratedArtifactParity(t *testing.T) {
	ctx := context.Background()
	h := sqlite.New(t)
	require.NoError(t, h.ExecStatements(ctx,
		"CREATE TABLE owners(id INTEGER PRIMARY KEY)",
		"CREATE TABLE records(id INTEGER PRIMARY KEY, owner_id INTEGER NOT NULL REFERENCES owners(id), name TEXT DEFAULT 'new')"))
	newComponent := func() *spec.Component {
		view := func(name string) *spec.View {
			return &spec.View{Name: name, Source: &spec.ViewSource{SQL: "SELECT id, owner_id, name FROM records"}}
		}
		return &spec.Component{
			Name: "Records", Key: spec.Key{Kind: spec.KindComponent, Name: "Records"},
			Routes:   []*spec.Route{{Method: "GET", Path: "/records"}},
			Settings: &spec.Settings{DefaultConnector: "main"},
			RootView: view("Records"), Views: []*spec.View{view("OtherRecords")},
		}
	}
	r := column.New(column.Connections{"main": h.DB})
	baseline, shared := newComponent(), newComponent()
	require.NoError(t, r.RefineRoot(ctx, baseline, nil, nil))
	require.NoError(t, r.RefineViews(ctx, baseline, nil, nil))
	compilation := r.BeginCompilation()
	require.NoError(t, compilation.RefineRoot(ctx, shared, nil, nil))
	require.NoError(t, compilation.RefineViews(ctx, shared, nil, nil))
	require.Equal(t, baseline, shared)
	var files [][]generate.EmittedFile
	for _, item := range []*spec.Component{baseline, shared} {
		dir := t.TempDir()
		result, err := generate.New(generate.Input{Component: item, PackageName: "records", TargetPackage: "example.com/records", SQLResources: true}).Generate(dir)
		require.NoError(t, err)
		for i := range result.Files {
			result.Files[i].Path, err = filepath.Rel(dir, result.Files[i].Path)
			require.NoError(t, err)
		}
		files = append(files, result.Files)
	}
	require.NotEmpty(t, files[0])
	require.Equal(t, files[0], files[1])
}
