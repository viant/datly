package mcp

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"testing/fstest"

	bindresource "github.com/viant/bindly/resource"
	dexec "github.com/viant/datly/exec"
	mcpresource "github.com/viant/datly/mcp/resource"
	"github.com/viant/mcp-protocol/schema"
	"github.com/xeipuuv/gojsonschema"
)

func TestSkillToolBridgePublishesListAndGetCompatibilityTools(t *testing.T) {
	store := bindresource.New()
	if err := store.Register("skills", fstest.MapFS{"review/SKILL.md": {Data: []byte("---\nname: review\ndescription: Review records.\nallowed-tools: records.read\n---\nReview.")}}); err != nil {
		t.Fatal(err)
	}
	service, err := New(Config{Invoker: &serviceInvoker{}, AuthorizeCatalogTool: func(context.Context, dexec.ComponentTarget, string) error { return fmt.Errorf("deny authored tools") }, Resources: store, Folders: []mcpresource.Folder{{Namespace: "skills", Root: ".", URIPrefix: "skill://studio/", Skills: []string{"review"}}}})
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
	if err := service.AuthorizeCatalogTool(context.Background(), SkillResourceReadTool, "discover"); err != nil {
		t.Fatalf("skill reader must be discoverable: %v", err)
	}
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

func TestSkillDiscoveryCompactAndAuthorized(t *testing.T) {
	store := bindresource.New()
	if err := store.Register("skills", fstest.MapFS{
		"read/SKILL.md":               {Data: []byte("---\nname: read\ndescription: Read records.\n---\nRead guidance.")},
		"read/references/contract.md": {Data: []byte("Full contract.")},
		"hidden/SKILL.md":             {Data: []byte("---\nname: hidden\ndescription: Hidden records.\n---\nHidden guidance.")},
	}); err != nil {
		t.Fatal(err)
	}
	service, err := New(Config{Invoker: &serviceInvoker{}, Resources: store, Folders: []mcpresource.Folder{{Namespace: "skills", Root: ".", URIPrefix: "skill://catalog/", Skills: []string{"read", "hidden"}}}, AuthorizeCatalogResource: func(_ context.Context, uri, action string) error {
		if strings.Contains(uri, "/hidden/") {
			return fmt.Errorf("denied")
		}
		return nil
	}})
	if err != nil {
		t.Fatal(err)
	}
	discover, ok := service.Registry().ToolRegistry.Get(SkillDiscoverTool)
	if !ok {
		t.Fatal("compact discovery tool missing")
	}
	result, protocolErr := discover.Handler(context.Background(), &schema.CallToolRequest{Params: schema.CallToolRequestParams{Name: SkillDiscoverTool, Arguments: map[string]interface{}{"query": "records", "limit": 1}}})
	if protocolErr != nil || result == nil {
		t.Fatalf("discovery failed: %v", protocolErr)
	}
	assertSkillToolSchema(t, discover.Metadata.OutputSchema, result.StructuredContent)
	text := fmt.Sprint(result.StructuredContent)
	if strings.Contains(text, "contract.md") || strings.Contains(text, "hidden") || !strings.Contains(text, "resourceCount") {
		t.Fatalf("unexpected compact response: %s", text)
	}
	if err := service.AuthorizeCatalogTool(context.Background(), SkillDiscoverTool, "discover"); err != nil {
		t.Fatalf("compact discovery denied: %v", err)
	}
	targeted, protocolErr := discover.Handler(context.Background(), &schema.CallToolRequest{Params: schema.CallToolRequestParams{Name: SkillDiscoverTool, Arguments: map[string]interface{}{"kind": "resources", "skillURI": "skill://catalog/read/SKILL.md", "query": "contract"}}})
	if protocolErr != nil || targeted == nil {
		t.Fatalf("targeted resources failed: %v", protocolErr)
	}
	assertSkillToolSchema(t, discover.Metadata.OutputSchema, targeted.StructuredContent)
	if !strings.Contains(fmt.Sprint(targeted.StructuredContent), "contract.md") || strings.Contains(fmt.Sprint(targeted.StructuredContent), "Full contract.") {
		t.Fatal("targeted descriptor discovery lost isolation")
	}
	get, _ := service.Registry().ToolRegistry.Get(SkillGetTool)
	full, protocolErr := get.Handler(context.Background(), &schema.CallToolRequest{Params: schema.CallToolRequestParams{Name: SkillGetTool, Arguments: map[string]interface{}{"uri": "skill://catalog/read/SKILL.md"}}})
	if protocolErr != nil || !strings.Contains(fmt.Sprint(full.StructuredContent), "contract.md") {
		t.Fatal("on-demand inventory lost reference contract")
	}
}
