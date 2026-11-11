package invocation

import (
	"context"

	xmcp "github.com/viant/xdatly/handler/mcp"
)

// WithMCPContext supplies the same protocol context during discovery and calls.
// Existing transport context retains its client and invocation ownership.
func WithMCPContext(ctx context.Context, client xmcp.Client) context.Context {
	if ctx == nil {
		return nil
	}
	if _, present := xmcp.LookupContext(ctx); present {
		return ctx
	}
	return xmcp.WithContext(ctx, &requestContext{client: client})
}
