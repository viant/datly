package docs_test

import (
	"context"
	"embed"
	"github.com/stretchr/testify/require"
	"github.com/viant/afs"
	afsio "github.com/viant/afs/adapter/io"
	"github.com/viant/bindly/resource"
	docs "github.com/viant/datly/documentation"
	xdocs "github.com/viant/xdatly/docs"
	"os"
	"path/filepath"
	"testing"
	"testing/fstest"
)

//go:embed testdata/*.yaml
var files embed.FS

func TestOrderedDictionaryResources(t *testing.T) {
	ctx := context.Background()
	store := resource.New()
	require.NoError(t, store.Register("pkg", files))
	loader := docs.Loader{Resources: store}
	for _, test := range []struct {
		name    string
		sources []xdocs.Source
		want    string
	}{
		{"global", []xdocs.Source{{DocURL: "pkg:testdata/global.yaml"}}, "User identifier"},
		{"rule", []xdocs.Source{{DocURL: "pkg:testdata/global.yaml"}, {DocURL: "pkg:testdata/rule.yaml"}}, "Rule user identifier"},
		{"ordered", []xdocs.Source{{BaseURL: "pkg:testdata", DocURL: "missing.yaml", DocURLs: []string{"rule.yaml", "global.yaml", "global.yaml"}}}, "User identifier"},
	} {
		t.Run(test.name, func(t *testing.T) {
			snapshot, err := loader.Load(ctx, test.sources...)
			require.NoError(t, err)
			require.Equal(t, test.want, snapshot.Field(docs.Field{Table: "USERS", Column: "ID"}).Description)
			require.Equal(t, "Order identifier", snapshot.Field(docs.Field{Table: "orders", Column: "id"}).Description)
			require.Equal(t, "Bare identifier", snapshot.Field(docs.Field{Table: "missing", Column: "id"}).Description)
			require.Equal(t, "User code", snapshot.Field(docs.Field{Table: "users", Column: "code"}).Description)
			require.Equal(t, "explicit", snapshot.Field(docs.Field{Table: "users", Column: "id", Authored: docs.Annotation{Description: "explicit"}}).Description)
			require.Equal(t, "Maximum rows", snapshot.Parameter("limit", docs.Annotation{}).Description)
			origins := snapshot.Origins()
			origins[0] = "changed"
			require.NotEqual(t, origins, snapshot.Origins())
			if test.name == "rule" {
				require.Empty(t, snapshot.Field(docs.Field{Table: "users", Column: "name"}).Description)
				require.Empty(t, snapshot.Field(docs.Field{Table: "users", Column: "id"}).Example)
			}
		})
	}
	dir := t.TempDir()
	require.NoError(t, os.WriteFile(filepath.Join(dir, "docs.yaml"), []byte("Paths:\n  /external: External files\n"), 0600))
	require.NoError(t, store.Register("external", os.DirFS(dir)))
	snapshot, err := loader.Load(ctx, xdocs.Source{DocURL: "external:docs.yaml"})
	require.NoError(t, err)
	require.Equal(t, "External files", snapshot.Operation("/external", docs.Annotation{}).Description)
}
func TestDictionaryRejectsAmbiguityAndInvalidResources(t *testing.T) {
	for _, test := range []struct{ name, text string }{
		{"empty", ""}, {"malformed", "Columns: ["}, {"duplicate", "Columns: {id: first, id: second}"},
		{"case conflict", "Columns: {ID: first, id: second}"}, {"nested conflict", "Columns: {users: {ID: first, id: second}}"},
		{"section conflict", "Columns: {}\ncolumns: {}"}, {"numeric description", "Columns: {id: 123}"},
		{"unknown", "Types: {}"}, {"multidocument", "Columns: {}\n---\nColumns: {}"},
	} {
		t.Run(test.name, func(t *testing.T) {
			store := resource.New()
			require.NoError(t, store.Register("p", fstest.MapFS{"d.yaml": {Data: []byte(test.text)}}))
			_, err := (docs.Loader{Resources: store}).Load(context.Background(), xdocs.Source{DocURL: "p:d.yaml"})
			require.Error(t, err)
		})
	}
	store := resource.New()
	require.NoError(t, store.Register("p", files))
	for _, ref := range []string{"missing.yaml", "p:missing.yaml", "", "p:../escape.yaml"} {
		_, err := (docs.Loader{Resources: store}).Load(context.Background(), xdocs.Source{DocURLs: []string{ref}})
		require.Error(t, err)
	}
}

// This acceptance currently exposes a defect in the pinned native AFS adapter.
// It remains an explicit integration gate; Datly must not add a fallback reader.
func TestAFSResourceAdapterAcceptance(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	require.NoError(t, os.WriteFile(filepath.Join(dir, "docs.yaml"), []byte("Paths:\n  /external: External files\n"), 0600))
	store := resource.New()
	require.NoError(t, store.Register("external", afsio.NewFS(ctx, afs.New(), "file://"+dir)))
	snapshot, err := (docs.Loader{Resources: store}).Load(ctx, xdocs.Source{DocURL: "external:docs.yaml"})
	require.NoError(t, err)
	require.Equal(t, "External files", snapshot.Operation("/external", docs.Annotation{}).Description)
}
