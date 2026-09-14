package invocation

import (
	"github.com/viant/mcp-protocol/schema"
	"github.com/viant/xdatly/response"
)

// transportResult preserves the HTTP body as explicit MCP content. A generic
// response never becomes structuredContent by inspecting its payload bytes.
func (e *Execution) transportResult(transport response.Response, payload []byte, failed bool) *schema.CallToolResult {
	media := transport.Headers().Get("Content-Type")
	if media == "" {
		media = "application/octet-stream"
	}
	result := &schema.CallToolResult{Content: []schema.CallToolResultContentElem{schema.NewEmbeddedBlob("datly://response/body", media, payload)}}
	if failed {
		result.IsError = &failed
	}
	return result
}
