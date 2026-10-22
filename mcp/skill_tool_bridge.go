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
	SkillListTool         = "skills_list"
	SkillGetTool          = "skills_get"
	SkillResourceReadTool = "skills_read_resource"
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

type skillResourceToolOutput struct {
	Contents []schema.ReadResourceResultContentsElem `json:"contents"`
}

func registerSkillToolBridge(registry *mcpserver.Registry, authorize func(context.Context, string, string) error, authorizeRead func(context.Context, string) error) error {
	if registry == nil || !registry.ImplementsSkills() {
		return nil
	}
	for _, name := range []string{SkillListTool, SkillGetTool, SkillResourceReadTool} {
		if _, exists := registry.ToolRegistry.Get(name); exists {
			return fmt.Errorf("MCP skill compatibility tool %q conflicts with an authored tool", name)
		}
	}
	if err := mcpserver.RegisterTool[skillListToolInput, skillListToolOutput](registry, SkillListTool, "List published MCP skills for clients without the Skills extension.", func(ctx context.Context, input skillListToolInput) (*schema.CallToolResult, *jsonrpc.Error) {
		result, err := listVisibleSkills(ctx, registry, authorize, input.Cursor)
		if err != nil {
			return nil, err
		}
		return structuredSkillToolResult(skillListToolOutput{ResultType: string(schema.ResultTypeComplete), Skills: result.Skills, NextCursor: result.NextCursor, TTLMillis: 0, CacheScope: "private"})
	}); err != nil {
		return err
	}
	if err := mcpserver.RegisterTool[skillGetToolInput, skillGetToolOutput](registry, SkillGetTool, "Get one published MCP skill manifest by its SKILL.md URI.", func(ctx context.Context, input skillGetToolInput) (*schema.CallToolResult, *jsonrpc.Error) {
		if input.URI == "" {
			return nil, jsonrpc.NewInvalidParamsError("skill URI is required", nil)
		}
		for _, entry := range registry.ListRegisteredSkills() {
			if entry.Uri == input.URI {
				if authorize != nil {
					if authorize(ctx, entry.Uri, "describe") != nil {
						return nil, jsonrpc.NewInvalidParamsError("unknown skill URI", nil)
					}
					for _, file := range entry.Resources.Files {
						if authorize(ctx, file.Uri, "describe") != nil {
							return nil, jsonrpc.NewInvalidParamsError("unknown skill URI", nil)
						}
					}
				}
				return structuredSkillToolResult(skillGetToolOutput{ResultType: string(schema.ResultTypeComplete), Skill: entry, TTLMillis: 0, CacheScope: "private"})
			}
		}
		return nil, jsonrpc.NewInvalidParamsError("unknown skill URI", nil)
	}); err != nil {
		return err
	}
	if err := mcpserver.RegisterTool[skillGetToolInput, skillResourceToolOutput](registry, SkillResourceReadTool, "Read a published skill's SKILL.md or supporting file. Use an exact resource URI returned by skills_list or skills_get.", func(ctx context.Context, input skillGetToolInput) (*schema.CallToolResult, *jsonrpc.Error) {
		visible := false
		for _, skill := range registry.ListRegisteredSkills() {
			for _, file := range skill.Resources.Files {
				if file.Uri == input.URI {
					visible = true
					break
				}
			}
		}
		if !visible || (authorize != nil && authorize(ctx, input.URI, "describe") != nil) || (authorizeRead != nil && authorizeRead(ctx, input.URI) != nil) {
			return nil, jsonrpc.NewInvalidParamsError("unknown skill resource URI", nil)
		}
		request := &schema.ReadResourceRequest{Params: schema.ReadResourceRequestParams{Uri: input.URI}}
		result, protocolErr, found := registry.ReadStaticSkillResource(ctx, request)
		if protocolErr != nil {
			return nil, protocolErr
		}
		if !found || result == nil {
			return nil, jsonrpc.NewInvalidParamsError("unknown skill resource URI", nil)
		}
		return structuredSkillToolResult(skillResourceToolOutput{Contents: result.Contents})
	}); err != nil {
		return err
	}
	read, _ := registry.ToolRegistry.Get(SkillResourceReadTool)
	read.Metadata.OutputSchema = &schema.ToolOutputSchema{Type: "object", Required: []string{"contents"}, Properties: schema.ToolInputSchemaProperties{"contents": map[string]interface{}{"type": "array", "items": map[string]interface{}{"type": "object", "required": []string{"uri"}, "properties": map[string]interface{}{"uri": map[string]interface{}{"type": "string"}, "text": map[string]interface{}{"type": "string"}, "blob": map[string]interface{}{"type": "string"}, "mimeType": map[string]interface{}{"type": "string"}}}}}}
	// SkillResources has a custom JSON representation (an inventory array or
	// "dynamic"). Reflection describes its Go struct instead of that wire union.
	// Keep compatibility tools accurate for clients that validate outputSchema.
	for _, name := range []string{SkillListTool, SkillGetTool} {
		entry, _ := registry.ToolRegistry.Get(name)
		entry.Metadata.OutputSchema = skillToolOutputSchema(name == SkillListTool)
	}
	return nil
}

func skillToolOutputSchema(list bool) *schema.ToolOutputSchema {
	resource := map[string]interface{}{"type": "object", "required": []string{"uri", "digest", "size"}, "properties": map[string]interface{}{
		"uri": map[string]interface{}{"type": "string"}, "digest": map[string]interface{}{"type": "string"}, "size": map[string]interface{}{"type": "integer", "minimum": 0},
	}}
	skill := map[string]interface{}{"type": "object", "required": []string{"uri", "frontmatter", "resources"}, "properties": map[string]interface{}{
		"uri": map[string]interface{}{"type": "string"}, "frontmatter": map[string]interface{}{"type": "object"},
		"resources": map[string]interface{}{"oneOf": []interface{}{map[string]interface{}{"type": "array", "items": resource}, map[string]interface{}{"type": "string", "enum": []string{"dynamic"}}}},
	}}
	properties := schema.ToolInputSchemaProperties{
		"resultType": map[string]interface{}{"type": "string", "enum": []string{"complete"}},
		"ttlMs":      map[string]interface{}{"type": "integer"}, "cacheScope": map[string]interface{}{"type": "string"},
	}
	key := "skill"
	if list {
		key = "skills"
		properties[key] = map[string]interface{}{"type": "array", "items": skill}
		properties["nextCursor"] = map[string]interface{}{"type": "string"}
	} else {
		properties[key] = skill
	}
	return &schema.ToolOutputSchema{Type: "object", Properties: properties, Required: []string{"resultType", key, "ttlMs", "cacheScope"}}
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
