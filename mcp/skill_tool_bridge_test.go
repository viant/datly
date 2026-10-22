package mcp

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"testing/fstest"

	bindresource "github.com/viant/bindly/resource"
	mcpresource "github.com/viant/datly/mcp/resource"
	"github.com/viant/mcp-protocol/schema"
	"github.com/xeipuuv/gojsonschema"
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
	assertSkillToolSchema(t, list.Metadata.OutputSchema, result.StructuredContent)
	get, ok := service.Registry().ToolRegistry.Get(SkillGetTool)
	if !ok {
		t.Fatal("skills/get compatibility tool missing")
	}
	result, protocolErr = get.Handler(context.Background(), &schema.CallToolRequest{Params: schema.CallToolRequestParams{Name: SkillGetTool, Arguments: map[string]interface{}{"uri": "skill://studio/review/SKILL.md"}}})
	if protocolErr != nil || result.StructuredContent == nil {
		t.Fatalf("get result=%+v error=%v", result, protocolErr)
	}
	assertSkillToolSchema(t, get.Metadata.OutputSchema, result.StructuredContent)
	read, ok := service.Registry().ToolRegistry.Get(SkillResourceReadTool)
	if !ok || strings.Contains(SkillResourceReadTool, "/") || strings.Contains(SkillListTool, "/") || strings.Contains(SkillGetTool, "/") {
		t.Fatal("skill compatibility tools must use portable tool names")
	}
	result, protocolErr = read.Handler(context.Background(), &schema.CallToolRequest{Params: schema.CallToolRequestParams{Name: SkillResourceReadTool, Arguments: map[string]interface{}{"uri": "skill://studio/review/SKILL.md"}}})
	if protocolErr != nil || result == nil {
		t.Fatalf("read error=%v", protocolErr)
	}
	assertSkillToolSchema(t, read.Metadata.OutputSchema, result.StructuredContent)
	if !strings.Contains(fmt.Sprint(result.StructuredContent), "Review records.") {
		t.Fatal("compatibility read lost the published skill text")
	}
}

func TestSkillResourceToolPreservesResourceReadAuthorization(t *testing.T) {
	store := bindresource.New()
	if err := store.Register("skills", fstest.MapFS{"private/SKILL.md": {Data: []byte("---\nname: private\ndescription: Private skill.\n---\nSecret guidance.")}}); err != nil {
		t.Fatal(err)
	}
	checked := ""
	service, err := New(Config{Invoker: &serviceInvoker{}, Resources: store, Folders: []mcpresource.Folder{{Namespace: "skills", Root: ".", URIPrefix: "skill://studio/", Skills: []string{"private"}}}, AuthorizeResource: func(_ context.Context, uri string) error { checked = uri; return fmt.Errorf("denied") }})
	if err != nil {
		t.Fatal(err)
	}
	read, _ := service.Registry().ToolRegistry.Get(SkillResourceReadTool)
	result, protocolErr := read.Handler(context.Background(), &schema.CallToolRequest{Params: schema.CallToolRequestParams{Name: SkillResourceReadTool, Arguments: map[string]interface{}{"uri": "skill://studio/private/SKILL.md"}}})
	if result != nil || protocolErr == nil || checked != "skill://studio/private/SKILL.md" {
		t.Fatal("resource read policy was bypassed")
	}
}

func assertSkillToolSchema(t *testing.T, contract *schema.ToolOutputSchema, value interface{}) {
	t.Helper()
	checked, err := gojsonschema.Validate(gojsonschema.NewGoLoader(contract), gojsonschema.NewGoLoader(value))
	if err != nil {
		t.Fatal(err)
	}
	if !checked.Valid() {
		t.Fatalf("skill compatibility response violates output schema: %v", checked.Errors())
	}
}

func TestSkillToolSchemaMatchesResourcesWireUnion(t *testing.T) {
	for _, resources := range []interface{}{[]interface{}{}, "dynamic"} {
		assertSkillToolSchema(t, skillToolOutputSchema(false), map[string]interface{}{
			"resultType": "complete", "ttlMs": 0, "cacheScope": "private",
			"skill": map[string]interface{}{"uri": "skill://review/SKILL.md", "frontmatter": map[string]interface{}{}, "resources": resources},
		})
	}
	invalid := map[string]interface{}{"resultType": "complete", "ttlMs": 0, "cacheScope": "private", "skill": map[string]interface{}{
		"uri": "skill://review/SKILL.md", "frontmatter": map[string]interface{}{}, "resources": map[string]interface{}{"Files": []interface{}{}},
	}}
	checked, err := gojsonschema.Validate(gojsonschema.NewGoLoader(skillToolOutputSchema(false)), gojsonschema.NewGoLoader(invalid))
	if err != nil || checked.Valid() {
		t.Fatalf("Go struct representation must not satisfy wire schema: %v, %v", checked, err)
	}
}
