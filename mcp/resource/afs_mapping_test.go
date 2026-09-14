package resource

import (
	"context"
	"io/fs"
	"testing"

	"github.com/viant/afs"
	afsio "github.com/viant/afs/adapter/io"
	afsresource "github.com/viant/afs/resource"
	bindresource "github.com/viant/bindly/resource"
	"github.com/viant/mcp-protocol/schema"
)

func TestAFSCustomSchemePublishesMappedSkillFiles(t *testing.T) {
	ctx := context.Background()
	provider, err := afsresource.NewProvider(afsresource.Config{Scheme: "skill", Roots: map[string]afsresource.Root{"application-guide": {FS: folderFiles, Path: "testdata/published"}}})
	if err != nil {
		t.Fatal(err)
	}
	files := afs.NewWithProviders(map[string]afs.Provider{"skill": provider})
	defer files.Close("skill://application-guide")
	store := bindresource.New()
	if err := store.Register("mounted", afsio.NewFS(ctx, files, "skill://application-guide")); err != nil {
		t.Fatal(err)
	}
	plans, err := (Publisher{Resources: store}).Compile(ctx, []Folder{{Namespace: "mounted", Root: ".", URIPrefix: "skill://application-guide/"}})
	if err != nil {
		t.Fatal(err)
	}
	catalog, err := NewCatalog(plans)
	if err != nil {
		t.Fatal(err)
	}
	if len(catalog.Resources()) != 2 {
		t.Fatalf("resources=%v", catalog.Resources())
	}
	handler := NewHandler(catalog, nil)
	for _, path := range []string{"SKILL.md", "references/guide.md"} {
		expected, err := fs.ReadFile(folderFiles, "testdata/published/"+path)
		if err != nil {
			t.Fatal(err)
		}
		actual, protocolErr := handler.Handle(ctx, &schema.ReadResourceRequest{Params: schema.ReadResourceRequestParams{Uri: "skill://application-guide/" + path}})
		if protocolErr != nil || len(actual.Contents) != 1 || actual.Contents[0].Text != string(expected) {
			t.Fatalf("%s content=%+v error=%v", path, actual, protocolErr)
		}
	}
	if _, err := files.DownloadWithURL(ctx, "skill://application-guide/../private.txt"); err == nil {
		t.Fatal("AFS mount allowed parent traversal")
	}
	if _, err := handler.Handle(ctx, &schema.ReadResourceRequest{Params: schema.ReadResourceRequestParams{Uri: "skill://application-guide/private.txt"}}); err == nil {
		t.Fatal("MCP published a file outside mapped root")
	}
}
