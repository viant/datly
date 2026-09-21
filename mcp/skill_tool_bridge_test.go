package mcp

import (
	"context"
	"testing"
	"testing/fstest"

	bindresource "github.com/viant/bindly/resource"
	mcpresource "github.com/viant/datly/mcp/resource"
	"github.com/viant/mcp-protocol/schema"
)

func TestSkillToolBridgePublishesListAndGetCompatibilityTools(t *testing.T) {
	store := bindresource.New()
	if err := store.Register("skills", fstest.MapFS{"review/SKILL.md": {Data: []byte("---\nname: review\ndescription: Review records.\nallowed-tools: records.read\n---\nReview.")}}); err != nil {
		t.Fatal(err)
	}
	service, err := New(Config{Invoker: &serviceInvoker{}, Resources: store, Folders: []mcpresource.Folder{{Namespace: "skills", Root: ".", URIPrefix: "skill://studio/", Skills: []string{"review"}}}})
	if err != nil {
		t.Fatal(err)
	}
	list, ok := service.Registry().ToolRegistry.Get(SkillListTool)
	if !ok || list.Metadata.OutputSchema == nil {
		t.Fatal("skills/list compatibility tool missing")
	}
	result, protocolErr := list.Handler(context.Background(), &schema.CallToolRequest{Params: schema.CallToolRequestParams{Name: SkillListTool}})
	if protocolErr != nil || result.StructuredContent == nil {
		t.Fatalf("list result=%+v error=%v", result, protocolErr)
	}
	get, ok := service.Registry().ToolRegistry.Get(SkillGetTool)
	if !ok {
		t.Fatal("skills/get compatibility tool missing")
	}
	result, protocolErr = get.Handler(context.Background(), &schema.CallToolRequest{Params: schema.CallToolRequestParams{Name: SkillGetTool, Arguments: map[string]interface{}{"uri": "skill://studio/review/SKILL.md"}}})
	if protocolErr != nil || result.StructuredContent == nil {
		t.Fatalf("get result=%+v error=%v", result, protocolErr)
	}
}
