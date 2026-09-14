package bootstrap

import (
	"io/fs"
	"testing"

	afsembed "github.com/viant/afs/embed"
	"github.com/viant/bindly/resource"
)

func TestDynamicEmbeddedResourcePaths(t *testing.T) {
	for _, paths := range [][]string{{"query.sql"}, {"a.txt", "z/query.sql"}, {"a/b/query.sql", "a/c/query.sql", "root.sql"}} {
		t.Run(paths[0], func(t *testing.T) {
			holder := afsembed.NewHolder()
			for _, path := range paths {
				holder.Add(path, "content:"+path)
			}
			snapshot := holder.EmbedFs()
			store := resource.New()
			if err := store.Register("generated", snapshot); err != nil {
				t.Fatal(err)
			}
			for _, path := range paths {
				actual, err := fs.ReadFile(store, "generated:"+path)
				if err != nil || string(actual) != "content:"+path {
					t.Fatalf("path=%s content=%q error=%v", path, actual, err)
				}
			}
		})
	}
}

func TestSeparateDynamicResourceGenerations(t *testing.T) {
	old := afsembed.NewHolder()
	old.Add("query.sql", "SELECT 1")
	previous := old.EmbedFs()
	next := afsembed.NewHolder()
	next.Add("query.sql", "SELECT 2")
	current := next.EmbedFs()
	for _, tt := range []struct {
		source fs.FS
		want   string
	}{{previous, "SELECT 1"}, {current, "SELECT 2"}} {
		actual, err := fs.ReadFile(tt.source, "query.sql")
		if err != nil || string(actual) != tt.want {
			t.Fatalf("content=%q error=%v", actual, err)
		}
	}
}
