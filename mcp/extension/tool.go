package extension

import (
	"context"
	"github.com/viant/mcp/schema"
)

type ToolHandlerFunc func(ctx context.Context, request *schema.CallToolRequestParams) (*schema.CallToolResult, error)
type Tool struct {
	schema.Tool
	Handler ToolHandlerFunc
}

// Tools is a collection of Tool
type Tools []*Tool
