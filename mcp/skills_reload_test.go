package mcp_test

import (
	"context"
	"github.com/viant/datly/application"
	"github.com/viant/datly/mcp"
	"github.com/viant/datly/mcp/resource"
	"github.com/viant/datly/typecatalog"
	"github.com/viant/mcp-protocol/schema"
	"reflect"
	"sync"
	"testing"
	"testing/fstest"
)

func TestSkillsReloadImmutableGeneration(t *testing.T) {
	ctx := context.Background()
	manager, err := application.New(nil)
	if err != nil {
		t.Fatal(err)
	}
	defer manager.Shutdown(ctx)
	files := fstest.MapFS{"SKILL.md": {Data: []byte("---\nname: app\ndescription: First.\n---\nfirst")}, "ref.txt": {Data: []byte("first ref")}}
	compile := func(context.Context, *typecatalog.Catalog) (*application.Build, error) {
		return &application.Build{MCP: mcp.Config{Folders: []resource.Folder{{Namespace: "docs", FS: files, URIPrefix: "skill://app/", Skills: []string{"."}}}}}, nil
	}
	if err = manager.Reload(ctx, application.Request{Revision: 1, Compile: compile}); err != nil {
		t.Fatal(err)
	}
	pinned, first, err := manager.Pin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	old := first.Registry().ListRegisteredSkills()
	files["SKILL.md"].Data = []byte("---\nname: app\ndescription: Second.\n---\nsecond")
	files["new.txt"] = &fstest.MapFile{Data: []byte("new")}
	if err = manager.Reload(ctx, application.Request{Revision: 2, Compile: compile}); err != nil {
		t.Fatal(err)
	}
	_, current, err := manager.Pin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if reflect.DeepEqual(old, current.Registry().ListRegisteredSkills()) {
		t.Fatal("metadata update missing")
	}
	var wg sync.WaitGroup
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_, retained, e := manager.Pin(pinned)
			if e != nil {
				t.Error(e)
				return
			}
			if !reflect.DeepEqual(old, retained.Registry().ListRegisteredSkills()) {
				t.Error("pinned metadata changed")
			}
			read, rpc := retained.ReadResource(pinned, &schema.ReadResourceRequest{Params: schema.ReadResourceRequestParams{Uri: "skill://app/SKILL.md"}})
			if rpc != nil || read.Contents[0].Text != "---\nname: app\ndescription: First.\n---\nfirst" {
				t.Error("pinned content changed")
			}
		}()
	}
	wg.Wait()
	files["SKILL.md"].Data = []byte("invalid")
	if err = manager.Reload(ctx, application.Request{Revision: 3, Compile: compile}); err == nil || manager.Revision() != 2 {
		t.Fatal("invalid skill generation published")
	}
}
