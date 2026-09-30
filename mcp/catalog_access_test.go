package mcp

import (
	"context"
	"fmt"
	"testing"
	"testing/fstest"

	bindresource "github.com/viant/bindly/resource"
	mcpresource "github.com/viant/datly/mcp/resource"
	native "github.com/viant/datly/mcp/server"
	"github.com/viant/jsonrpc"
	"github.com/viant/mcp-protocol/schema"
	protocol "github.com/viant/mcp-protocol/server"
)

type catalogAccessKey struct{}

func TestCatalogAccessProtectsNativeAndCompatibilitySkillPaths(t *testing.T) {
	ctx := context.Background()
	store := bindresource.New()
	if err := store.Register("skills", fstest.MapFS{"SKILL.md": {Data: []byte("---\nname: private\ndescription: Protected source.\n---\nprivate bytes")}}); err != nil {
		t.Fatal(err)
	}
	guard := func(ctx context.Context, _ string, _ string) error {
		if ctx.Value(catalogAccessKey{}) != true {
			return fmt.Errorf("denied")
		}
		return nil
	}
	service, err := New(Config{Invoker: &serviceInvoker{}, Resources: store, Folders: []mcpresource.Folder{{Namespace: "skills", Root: ".", URIPrefix: "skill://private/", Skills: []string{"."}}}, AuthorizeCatalogResource: guard, AuthorizeResource: func(ctx context.Context, uri string) error { return guard(ctx, uri, "retrieve") }})
	if err != nil {
		t.Fatal(err)
	}
	factory, err := native.NewHandler(service)
	if err != nil {
		t.Fatal(err)
	}
	handler, err := factory(ctx, nil, nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	skills := handler.(protocol.Skills)
	uri := "skill://private/SKILL.md"
	for _, allowed := range []bool{false, true} {
		requestCtx := context.WithValue(ctx, catalogAccessKey{}, allowed)
		list, e := skills.ListSkills(requestCtx, nil)
		if e != nil {
			t.Fatal(e)
		}
		expected := 0
		if allowed {
			expected = 1
		}
		if len(list.Skills) != expected {
			t.Fatalf("allowed=%v skills=%d", allowed, len(list.Skills))
		}
		resources, e := handler.ListResources(requestCtx, nil)
		if e != nil || len(resources.Resources) != expected {
			t.Fatalf("allowed=%v resources=%+v err=%v", allowed, resources, e)
		}
		_, e = skills.GetSkill(requestCtx, &jsonrpc.TypedRequest[*schema.GetSkillRequest]{Request: &schema.GetSkillRequest{Params: schema.GetSkillRequestParams{Uri: uri}}})
		if (e == nil) != allowed {
			t.Fatalf("allowed=%v get error=%v", allowed, e)
		}
		read, e := handler.ReadResource(requestCtx, &jsonrpc.TypedRequest[*schema.ReadResourceRequest]{Request: &schema.ReadResourceRequest{Params: schema.ReadResourceRequestParams{Uri: uri}}})
		if (e == nil) != allowed {
			t.Fatalf("allowed=%v static read=%+v err=%v", allowed, read, e)
		}
		for _, name := range []string{SkillListTool, SkillGetTool} {
			tool, _ := service.Registry().ToolRegistry.Get(name)
			args := map[string]interface{}{}
			if name == SkillGetTool {
				args["uri"] = uri
			}
			result, e := tool.Handler(requestCtx, &schema.CallToolRequest{Params: schema.CallToolRequestParams{Name: name, Arguments: args}})
			if name == SkillGetTool {
				if (e == nil) != allowed {
					t.Fatalf("allowed=%v bridge get error=%v", allowed, e)
				}
			} else {
				if e != nil {
					t.Fatal(e)
				}
				entries := result.StructuredContent.(map[string]interface{})["skills"].([]interface{})
				if len(entries) != expected {
					t.Fatalf("allowed=%v bridge skills=%d", allowed, len(entries))
				}
			}
		}
	}
}

func TestVisibleSkillsPaginateAfterFilteringAndInvalidateChangedAccess(t *testing.T) {
	files := fstest.MapFS{}
	roots := []string{}
	for i := 0; i < 33; i++ {
		root := fmt.Sprintf("skill-%02d", i)
		roots = append(roots, root)
		files[root+"/SKILL.md"] = &fstest.MapFile{Data: []byte(fmt.Sprintf("---\nname: %s\ndescription: Fixture.\n---\nfixture", root))}
	}
	store := bindresource.New()
	if err := store.Register("skills", files); err != nil {
		t.Fatal(err)
	}
	guard := func(ctx context.Context, _ string, _ string) error {
		if ctx.Value(catalogAccessKey{}) != true {
			return fmt.Errorf("denied")
		}
		return nil
	}
	service, err := New(Config{Invoker: &serviceInvoker{}, Resources: store, Folders: []mcpresource.Folder{{Namespace: "skills", Root: ".", URIPrefix: "skill://paged/", Skills: roots}}, AuthorizeCatalogResource: guard})
	if err != nil {
		t.Fatal(err)
	}
	allowed := context.WithValue(context.Background(), catalogAccessKey{}, true)
	first, e := service.ListVisibleSkills(allowed, nil)
	if e != nil || len(first.Skills) != 32 || first.NextCursor == nil {
		t.Fatalf("first=%+v err=%v", first, e)
	}
	second, e := service.ListVisibleSkills(allowed, first.NextCursor)
	if e != nil || len(second.Skills) != 1 || second.NextCursor != nil {
		t.Fatalf("second=%+v err=%v", second, e)
	}
	if _, e = service.ListVisibleSkills(context.Background(), first.NextCursor); e == nil {
		t.Fatal("cursor from a permitted catalog survived changed access")
	}
	denied, e := service.ListVisibleSkills(context.Background(), nil)
	if e != nil || len(denied.Skills) != 0 {
		t.Fatalf("denied=%+v err=%v", denied, e)
	}
}
