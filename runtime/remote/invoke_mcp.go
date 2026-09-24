package remote

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"

	"github.com/viant/mcp-protocol/schema"
	xmcp "github.com/viant/xdatly/client/mcp"
)

// invokeMCP calls the configured tool through the borrowed client with the
// explicit header set and mapped arguments, then extracts the configured
// document from the native result. A tool isError result becomes *ToolError;
// protocol and transport errors are returned as the client reported them.
func invokeMCP(ctx context.Context, client xmcp.Client, p *plan, m *materials) (any, error) {
	header, arguments := p.outbound(m)
	request := &schema.CallToolRequest{
		Method: schema.MethodToolsCall,
		Params: schema.CallToolRequestParams{Name: p.tool, Arguments: arguments},
	}
	result, err := client.CallTool(ctx, request, xmcp.CallOptions{Header: header})
	if err != nil {
		return nil, err
	}
	if result == nil {
		return nil, fmt.Errorf("mcp tool %s at %s returned no result", p.tool, p.url)
	}
	if result.IsError != nil && *result.IsError {
		return nil, &ToolError{Tool: p.tool, URL: p.url, Message: textContent(result.Content)}
	}
	switch p.source {
	case SourceText:
		text := textContent(result.Content)
		if text == "" {
			return nil, fmt.Errorf("mcp tool %s returned no text content", p.tool)
		}
		return decodeJSONDocument([]byte(strings.TrimSpace(text)))
	default:
		if result.StructuredContent == nil {
			return nil, fmt.Errorf("mcp tool %s returned no structuredContent", p.tool)
		}
		return normalizeDocument(result.StructuredContent)
	}
}

// ToolError reports a tool-level failure (isError=true) as distinct from a
// protocol or transport failure.
type ToolError struct {
	Tool    string
	URL     string
	Message string
}

func (e *ToolError) Error() string {
	if e.Message == "" {
		return fmt.Sprintf("mcp tool %s at %s reported an error", e.Tool, e.URL)
	}
	return fmt.Sprintf("mcp tool %s at %s reported an error: %s", e.Tool, e.URL, e.Message)
}

func textContent(content []schema.CallToolResultContentElem) string {
	var parts []string
	for _, element := range content {
		switch actual := element.(type) {
		case schema.TextContent:
			parts = append(parts, actual.Text)
		case *schema.TextContent:
			if actual != nil {
				parts = append(parts, actual.Text)
			}
		case map[string]any:
			if kind, _ := actual["type"].(string); kind == "text" {
				if text, ok := actual["text"].(string); ok {
					parts = append(parts, text)
				}
			}
		}
	}
	return strings.Join(parts, "\n")
}

// normalizeDocument re-encodes structured content so pointer evaluation sees
// the same generic JSON shapes (json.Number, map, slice) as HTTP responses.
func normalizeDocument(value any) (any, error) {
	encoded, err := json.Marshal(value)
	if err != nil {
		return nil, fmt.Errorf("encode structuredContent: %w", err)
	}
	return decodeJSONDocument(encoded)
}
