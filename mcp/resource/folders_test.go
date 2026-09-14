package resource

import (
	"context"
	"embed"
	bindresource "github.com/viant/bindly/resource"
	route "github.com/viant/datly/runtime/route"
	"github.com/viant/mcp-protocol/schema"
	"os"
	"path/filepath"
	"testing"
	"testing/fstest"
)

//go:embed testdata
var folderFiles embed.FS

func TestMappedEmbeddedFoldersAndConfinement(t *testing.T) {
	store := bindresource.New()
	if err := store.Register("app", folderFiles); err != nil {
		t.Fatal(err)
	}
	plans, err := (Publisher{Resources: store}).Compile(context.Background(), []Folder{{Namespace: "app", Root: "testdata/published", URIPrefix: "skill://application/"}, {Namespace: "second", FS: folderFiles, Root: "testdata/published/references", URIPrefix: "docs://application/reference/"}})
	if err != nil {
		t.Fatal(err)
	}
	catalog, err := NewCatalog(plans)
	if err != nil {
		t.Fatal(err)
	}
	if len(catalog.Resources()) != 3 {
		t.Fatal("folder inventory mismatch")
	}
	handler := NewHandler(catalog, nil)
	for _, uri := range []string{"skill://application/SKILL.md", "skill://application/references/guide.md", "docs://application/reference/guide.md"} {
		result, err := handler.Handle(context.Background(), &schema.ReadResourceRequest{Params: schema.ReadResourceRequestParams{Uri: uri}})
		if err != nil || len(result.Contents) != 1 || result.Contents[0].Text == "" {
			t.Fatalf("%s: %v %+v", uri, err, result)
		}
	}
	for _, uri := range []string{"skill://application/../private.txt", "skill://application/%2e%2e/private.txt", "skill://application/references%2fguide.md", "skill://application/private.txt", "skill://application/SKILL.md?path=private", "skill://application/references/./guide.md"} {
		if _, err := handler.Handle(context.Background(), &schema.ReadResourceRequest{Params: schema.ReadResourceRequestParams{Uri: uri}}); err == nil {
			t.Fatalf("unpublished URI %s", uri)
		}
	}
	metadata := catalog.Resources()[0]
	if metadata.Size == nil || metadata.Meta["datly.sha256"] == nil {
		t.Fatal("missing size/hash")
	}
	*metadata.Size = 0
	delete(metadata.Meta, "datly.sha256")
	if *catalog.Resources()[0].Size == 0 || catalog.Resources()[0].Meta["datly.sha256"] == nil {
		t.Fatal("mutable metadata escaped")
	}
	if _, err := NewCatalog(append(plans, plans[0])); err == nil {
		t.Fatal("duplicate resource accepted")
	}
	pattern, err := route.CompilePathTemplate("/{file}")
	if err != nil {
		t.Fatal(err)
	}
	template := &Plan{scheme: "skill", authority: "application", path: pattern, template: &schema.ResourceTemplate{Name: "component", UriTemplate: "skill://application/{file}"}}
	if _, err := NewCatalog(append(plans, template)); err == nil {
		t.Fatal("template/file overlap accepted")
	}
}

func TestFolderDevelopmentSymlinksAndSnapshots(t *testing.T) {
	root := t.TempDir()
	outside := filepath.Join(t.TempDir(), "secret.txt")
	if err := os.WriteFile(outside, []byte("private"), 0600); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(outside, filepath.Join(root, "link")); err != nil {
		t.Fatal(err)
	}
	if _, err := (Publisher{}).Compile(context.Background(), []Folder{{Namespace: "dev", LocalDir: root, URIPrefix: "docs://dev/"}}); err == nil {
		t.Fatal("symlink published")
	}
	files := fstest.MapFS{"a.md": &fstest.MapFile{Data: []byte("old")}}
	folder := Folder{Namespace: "app", FS: files, URIPrefix: "docs://app/"}
	plans, err := (Publisher{}).Compile(context.Background(), []Folder{folder})
	if err != nil {
		t.Fatal(err)
	}
	files["a.md"].Data = []byte("new")
	old, _ := NewCatalog(plans)
	result, e := NewHandler(old, nil).Handle(context.Background(), &schema.ReadResourceRequest{Params: schema.ReadResourceRequestParams{Uri: "docs://app/a.md"}})
	if e != nil || result.Contents[0].Text != "old" {
		t.Fatal("snapshot changed")
	}
	current, err := (Publisher{}).Compile(context.Background(), []Folder{folder})
	if err != nil {
		t.Fatal(err)
	}
	if current[0].resource.Meta["datly.sha256"] == plans[0].resource.Meta["datly.sha256"] {
		t.Fatal("content version unchanged")
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err = (Publisher{}).Compile(ctx, []Folder{folder}); err == nil {
		t.Fatal("canceled snapshot accepted")
	}
}
