package mcp

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/viant/jsonrpc"
	"github.com/viant/mcp-protocol/schema"
	mcpserver "github.com/viant/mcp-protocol/server"
)

const (
	SkillListTool = "skills/list"
	SkillGetTool  = "skills/get"
)

type skillListToolInput struct {
	Cursor *string `json:"cursor,omitempty"`
}

type skillListToolOutput struct {
	ResultType string         `json:"resultType"`
	Skills     []schema.Skill `json:"skills"`
	NextCursor *string        `json:"nextCursor,omitempty"`
	TTLMillis  int            `json:"ttlMs"`
	CacheScope string         `json:"cacheScope"`
}

type skillGetToolInput struct {
	URI string `json:"uri"`
}

type skillGetToolOutput struct {
	ResultType string       `json:"resultType"`
	Skill      schema.Skill `json:"skill"`
	TTLMillis  int          `json:"ttlMs"`
	CacheScope string       `json:"cacheScope"`
}

func registerSkillToolBridge(registry *mcpserver.Registry) error {
	if registry == nil || !registry.ImplementsSkills() {
		return nil
	}
	for _, name := range []string{SkillListTool, SkillGetTool} {
		if _, exists := registry.ToolRegistry.Get(name); exists {
			return fmt.Errorf("MCP skill compatibility tool %q conflicts with an authored tool", name)
		}
	}
	if err := mcpserver.RegisterTool[skillListToolInput, skillListToolOutput](registry, SkillListTool, "List published MCP skills for clients without the Skills extension.", func(_ context.Context, _ skillListToolInput) (*schema.CallToolResult, *jsonrpc.Error) {
		return structuredSkillToolResult(skillListToolOutput{ResultType: string(schema.ResultTypeComplete), Skills: registry.ListRegisteredSkills(), TTLMillis: 0, CacheScope: "private"})
	}); err != nil {
		return err
	}
	return mcpserver.RegisterTool[skillGetToolInput, skillGetToolOutput](registry, SkillGetTool, "Get one published MCP skill manifest by its SKILL.md URI.", func(_ context.Context, input skillGetToolInput) (*schema.CallToolResult, *jsonrpc.Error) {
		if input.URI == "" {
			return nil, jsonrpc.NewInvalidParamsError("skill URI is required", nil)
		}
		for _, entry := range registry.ListRegisteredSkills() {
			if entry.Uri == input.URI {
				return structuredSkillToolResult(skillGetToolOutput{ResultType: string(schema.ResultTypeComplete), Skill: entry, TTLMillis: 0, CacheScope: "private"})
			}
		}
		return nil, jsonrpc.NewInvalidParamsError("unknown skill URI", nil)
	})
}

func structuredSkillToolResult(value interface{}) (*schema.CallToolResult, *jsonrpc.Error) {
	raw, err := json.Marshal(value)
	if err != nil {
		return nil, jsonrpc.NewInternalError("skill metadata encoding failed", nil)
	}
	var object map[string]interface{}
	if err = json.Unmarshal(raw, &object); err != nil {
		return nil, jsonrpc.NewInternalError("skill metadata encoding failed", nil)
	}
	failed := false
	return &schema.CallToolResult{Content: []schema.CallToolResultContentElem{schema.TextContent{Type: "text", Text: string(raw)}}, StructuredContent: object, IsError: &failed, ResultType: schema.ResultTypeComplete}, nil
}
