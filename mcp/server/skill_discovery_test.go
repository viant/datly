package server

import (
	"context"
	"encoding/json"
	"net/http"
	"strings"
	"testing"
	"testing/fstest"

	skillformat "github.com/viant/mcp-protocol/extension/skills"
	"github.com/viant/mcp-protocol/schema"
)

func TestSkillDiscoveryInstructionsOnConnection(t *testing.T) {
	for _, withSkill := range []bool{false, true} {
		t.Run(map[bool]string{false: "no skills", true: "published skills"}[withSkill], func(t *testing.T) {
			service := newTransportTestService(nil)
			if withSkill {
				compiled, err := (skillformat.Compiler{Source: fstest.MapFS{"SKILL.md": {Data: []byte("---\nname: guide\ndescription: Query guidance.\n---\nUse bounded queries.")}}}).Compile(context.Background(), "skill://guide/SKILL.md")
				if err != nil {
					t.Fatal(err)
				}
				if err = service.Registry().RegisterStaticSkill(compiled); err != nil {
					t.Fatal(err)
				}
			}
			wire := startWireServer(t, service)
			response, _, status := postWireRequest(t, wire.baseURL+"/mcp", map[string]interface{}{
				"jsonrpc": "2.0", "id": 1, "method": schema.MethodInitialize,
				"params": map[string]interface{}{"protocolVersion": schema.LegacyProtocolVersion, "capabilities": map[string]interface{}{}, "clientInfo": map[string]interface{}{"name": "test", "version": "1"}},
			}, "", "")
			if status != http.StatusOK || response.Error != nil {
				t.Fatalf("initialize failed: %d %+v", status, response.Error)
			}
			var initialized schema.InitializeResult
			if err := json.Unmarshal(response.Result, &initialized); err != nil {
				t.Fatal(err)
			}
			assertSkillInstructions(t, initialized.Instructions, withSkill)
			factory, _ := NewHandler(service)
			handler, _ := factory(context.Background(), nil, nil, nil)
			discovered := schema.DiscoverResult{}
			handler.(*Handler).Discover(context.Background(), &discovered)
			assertSkillInstructions(t, discovered.Instructions, withSkill)
		})
	}
}
func assertSkillInstructions(t *testing.T, instructions *string, want bool) {
	t.Helper()
	if !want {
		if instructions != nil {
			t.Fatalf("advertised absent skills: %q", *instructions)
		}
		return
	}
	if instructions == nil || !strings.Contains(*instructions, "skills/list") || !strings.Contains(*instructions, "skills/get") {
		t.Fatalf("missing discovery guidance: %v", instructions)
	}
	if strings.Contains(*instructions, "skill://guide") {
		t.Fatal("connection guidance exposed skill inventory")
	}
	for _, guidance := range []string{"resource inventory, not its text", "resources/read", "Supporting files are resources, not additional skills"} {
		if !strings.Contains(*instructions, guidance) {
			t.Fatalf("missing resource reading guidance %q: %s", guidance, *instructions)
		}
	}
}
func TestSkillDiscoveryInstructionsPreserveHostGuidance(t *testing.T) {
	text := "Host guidance."
	capabilities := schema.ServerCapabilities{Extensions: map[string]map[string]interface{}{schema.SkillsExtension: {}}}
	instructions := &text
	publishSkillDiscoveryInstructions(&capabilities, &instructions)
	publishSkillDiscoveryInstructions(&capabilities, &instructions)
	if !strings.HasPrefix(*instructions, "Host guidance.\n\n") || strings.Count(*instructions, skillDiscoveryInstructions) != 1 {
		t.Fatalf("guidance overwritten or duplicated: %s", *instructions)
	}
}
